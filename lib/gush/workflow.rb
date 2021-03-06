require 'securerandom'

module Gush
  class Workflow
    attr_accessor :id, :jobs, :stopped, :persisted, :arguments, :dependencies

    def initialize(*args)
      @id           = id
      @jobs         = []
      @dependencies = []
      @persisted    = false
      @stopped      = false
      @arguments    = args

      setup
    end

    def key
      "gush.workflows.#{id}"
    end

    def self.find(id)
      Gush::Client.new.find_workflow(id)
    end

    def self.create(*args)
      flow = new(*args)
      flow.save
      flow
    end

    def self.create!(*args)
      flow = self.create *args
      flow.start!
      flow
    end

    def self.create_and_wait!(*args)
      flow = self.create *args
      flow.start_and_wait!
    end

    def continue
      client      = Gush::Client.new
      failed_jobs = jobs.select(&:failed?)

      failed_jobs.each do |job|
        client.enqueue_job(id, job)
      end
    end

    def save
      persist!
    end

    def configure(*args)
    end

    def mark_as_stopped
      @stopped = true
    end

    def start!
      client.start_workflow(self)
    end

    def persist!
      client.persist_workflow(self)
    end

    def expire! (ttl = nil)
      client.expire_workflow(self, ttl)
    end

    def mark_as_persisted
      @persisted = true
    end

    def mark_as_started
      @stopped = false
    end

    def resolve_dependencies
      @dependencies.each do |dependency|
        from = find_job(dependency[:from])
        to   = find_job(dependency[:to])

        to.incoming << dependency[:from]
        from.outgoing << dependency[:to]
      end
    end

    def find_job(name)
      match_data = /(?<klass>\w*[^-])-(?<identifier>.*)/.match(name.to_s)

      if match_data.nil?
        job = jobs.find { |node| node.klass.to_s == name.to_s }
      else
        job = jobs.find { |node| node.name.to_s == name.to_s }
      end

      job
    end

    def started?
      !!started_at
    end

    def finished?
      jobs.all?(&:finished?)
    end

    def succeeded?
      jobs.all?(&:succeeded?)
    end

    def running?
      started? && !finished?
    end

    def retrying?
      jobs.any?(&:retrying?)
    end

    def failed?
      jobs.any?(&:failed?)
    end

    def stopped?
      stopped
    end

    def enqueued?
      jobs.any?(&:enqueued?)
    end

    TERMINAL_STATES = %i[succeeded failed stopped].freeze

    def status
      return :succeeded if succeeded?
      return :retrying if retrying?
      return :failed if failed?
      return :stopped if stopped?
      return :enqueued if enqueued?
      return :running if started?
      :pending
    end

    def run(klass, opts = {})
      node = klass.new({
                         workflow_id: id,
                         id:          client.next_free_job_id(id, klass.to_s),
                         params:      opts.fetch(:params, {}),
                         queue:       opts[:queue]
                       })

      jobs << node

      deps_after = [*opts[:after]]

      deps_after.each do |dep|
        @dependencies << { from: dep.to_s, to: node.name.to_s }
      end

      deps_before = [*opts[:before]]

      deps_before.each do |dep|
        @dependencies << { from: node.name.to_s, to: dep.to_s }
      end

      node.name
    end

    def reload
      flow = self.class.find(id)

      self.jobs    = flow.jobs
      self.stopped = flow.stopped

      self
    end

    def initial_jobs
      jobs.select(&:has_no_dependencies?)
    end

    def started_at
      first_job ? first_job.started_at : nil
    end

    def finished_at
      last_job ? last_job.finished_at : nil
    end

    def to_hash
      name = self.class.to_s
      {
        name:        name,
        id:          id,
        arguments:   @arguments,
        total:       jobs.count,
        finished:    jobs.count(&:finished?),
        klass:       name,
        status:      status,
        stopped:     stopped,
        started_at:  started_at,
        finished_at: finished_at
      }
    end

    def wait(n: 10, delay: 1, debug: false)
      n.times do
        self.reload
        puts "Workflow #{self.class} #{@id}, status: #{self.status}" if debug
        break if self.finished?
        sleep delay
      end
    end

    def start_and_wait!(**kwargs)
      self.start!
      self.wait **kwargs
      self
    end

    def to_json(options = {})
      Gush::JSON.encode(to_hash, options)
    end

    def self.descendants
      ObjectSpace.each_object(Class).select { |klass| klass < self }
    end

    def id
      @id ||= client.next_free_workflow_id
    end

    private

    def setup
      configure(*@arguments)
      resolve_dependencies
    end

    def client
      @client ||= Client.new
    end

    def first_job
      jobs.min_by { |n| n.started_at || Time.now.to_i }
    end

    def last_job
      jobs.max_by { |n| n.finished_at || 0 } if finished?
    end
  end
end
