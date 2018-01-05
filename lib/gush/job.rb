module Gush
  class Job
    attr_accessor :workflow_id, :incoming, :outgoing, :params,
      :finished_at, :failed_at, :started_at, :enqueued_at, :payloads, :klass, :queue
    attr_reader :name, :output_payload, :params, :error

    def initialize(workflow, opts = {})
      @workflow = workflow
      options = opts.dup
      assign_variables(options)
    end

    def payload(clazz)
        payload = payloads.detect { |f| f[:class] == clazz.name }
        raise "Unable to find payload for #{clazz}, available: #{payloads.collect { |f| f[:class]}}" unless payload
        payload[:output]
    end

    def as_json
      {
        name: name,
        klass: self.class.to_s,
        queue: queue,
        incoming: incoming,
        outgoing: outgoing,
        finished_at: finished_at,
        enqueued_at: enqueued_at,
        started_at: started_at,
        failed_at: failed_at,
        params: params,
        output_payload: output_payload,
        error: error
      }
    end

    def to_json(options = {})
      Gush::JSON.encode(as_json, options)
    end

    def self.from_hash(flow, hash)
      hash[:klass].constantize.new(flow, hash)
    end

    def output(data)
      @output_payload = data
    end

    def perform
    end

    def start!
      @started_at = current_timestamp
    end

    def enqueue!
      @enqueued_at = current_timestamp
      @started_at = nil
      @finished_at = nil
      @failed_at = nil
    end

    def finish!
      @finished_at = current_timestamp
    end

    def fail!(error = nil)
      @finished_at = @failed_at = current_timestamp
      @error = error
    end

    def enqueued?
      !enqueued_at.nil?
    end

    def finished?
      !finished_at.nil?
    end

    def failed?
      !failed_at.nil?
    end

    def succeeded?
      finished? && !failed?
    end

    def started?
      !started_at.nil?
    end

    def running?
      started? && !finished?
    end

    def ready_to_start?
      !running? && !enqueued? && !finished? && !failed? && parents_succeeded?
    end

    def parents_succeeded?
      incoming.all? do |name|
        @workflow.find_job(name).succeeded?
      end
    end

    def has_no_dependencies?
      incoming.empty?
    end

    private
    def logger
        Rails.logger
    end

    def current_timestamp
      Time.now.to_i
    end

    def assign_variables(opts)
      @name           = opts[:name]
      @incoming       = opts[:incoming] || []
      @outgoing       = opts[:outgoing] || []
      @failed_at      = opts[:failed_at]
      @finished_at    = opts[:finished_at]
      @started_at     = opts[:started_at]
      @enqueued_at    = opts[:enqueued_at]
      @params         = opts[:params] || {}
      @klass          = opts[:klass]
      @output_payload = opts[:output_payload]
      @queue          = opts[:queue]
      @error          = opts[:error]
    end
  end
end
