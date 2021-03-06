require 'sidekiq'
require 'redis-mutex'

module Gush
  class Worker
    include Sidekiq::Worker

    sidekiq_retries_exhausted do |args, ex|
      worker = self.new
      worker.setup_job *args['args']
      worker.fail! ex.message
    end

    def perform(workflow_id, job_id)
      setup_job(workflow_id, job_id)
      job.payloads = incoming_payloads
      start!

      begin
        job.perform
      rescue Job::AbortError => abort
        exception = abort.exception
        fail! exception.to_s
        # Don't reraise the exception, but log it on Sentry if any
        Raven.capture_exception(exception) if defined?(Raven)
      rescue => error
        error! error.to_s
        raise
      else
        succeed!
        enqueue_outgoing_jobs
      end
    end

    attr_reader :client, :workflow_id, :job

    def client
      @client ||= Gush::Client.new(Gush.configuration)
    end

    def setup_job(workflow_id, job_id)
      @workflow_id = workflow_id
      @job         ||= client.find_job(workflow_id, job_id)
      raise "Job not found #{workflow_id}/#{job_id}" unless @job
      clazz = @job.class
      if clazz.respond_to? :sidekiq_retry_in_block
        retry_in = clazz.sidekiq_retry_in_block
        self.class.sidekiq_retry_in &retry_in if retry_in
      end
      @job
    end

    def incoming_payloads
      job.incoming.map do |job_name|
        job = client.find_job(workflow_id, job_name)
        {
          id:     job.name,
          class:  job.klass.to_s,
          output: job.output_payload
        }
      end
    end

    def start!
      job.start!
      client.persist_job(job)
    end

    def succeed!
      job.succeed!
      client.persist_job(job)
    end

    def error!(error)
      job.error!(error)
      client.persist_job(job)
    end

    def fail!(error)
      job.fail!(error)
      client.persist_job(job)
    end

    def elapsed(start)
      (Time.now - start).to_f.round(3)
    end

    def enqueue_outgoing_jobs
      job.outgoing.each do |job_name|
        RedisMutex.with_lock("gush_enqueue_outgoing_jobs_#{workflow_id}-#{job_name}", sleep: 0.3, block: 2) do
          out = client.find_job(workflow_id, job_name)

          if out.ready_to_start?
            client.enqueue_job(workflow_id, out)
          end
        end
      end
    end
  end
end
