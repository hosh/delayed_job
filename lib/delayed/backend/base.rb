module Delayed
  module Backend
    module Base
      def self.included(base)
        base.extend ClassMethods
      end

      module ClassMethods
        # Add a job to the queue
        def enqueue(_handler, arguments = {}, options = {})
          options = { :priority => Delayed::Worker.default_priority }.merge(options)

          options[:handler_object] = _handler
          options[:arguments] = arguments

          raise ArgumentError, 'Cannot enqueue items which do not respond to call' unless options[:handler_object].respond_to?(:call)
          raise ArgumentError, 'Arguments must be a Hash' unless options[:arguments].is_a?(Hash) && options[:arguments].respond_to?(:to_json)

          if Delayed::Worker.delay_jobs || options[:run_at]
            self.create(options).tap do |job|
              job.hook(:enqueue)
            end
          else
            options[:handler_object].call(options[:arguments])
          end
        end

        def reserve(worker, max_run_time = Worker.max_run_time)
          # We get up to 5 jobs from the db. In case we cannot get exclusive access to a job we try the next.
          # this leads to a more even distribution of jobs across the worker processes
          find_available(worker.name, 5, max_run_time).detect do |job|
            job.lock_exclusively!(max_run_time, worker.name)
          end
        end

        # Hook method that is called before a new worker is forked
        def before_fork
        end

        # Hook method that is called after a new worker is forked
        def after_fork
        end

        def work_off(num = 100)
          warn "[DEPRECATION] `Delayed::Job.work_off` is deprecated. Use `Delayed::Worker.new.work_off instead."
          Delayed::Worker.new.work_off(num)
        end
      end

      def failed?
        failed_at
      end
      alias_method :failed, :failed?

      def name
        @name ||= handler_object.respond_to?(:display_name) ? handler_object.display_name : (handler_object.is_a?(Class) ? handler_object.name : handler_object.class.name )
      end

      def arguments=(hash)
        @arguments = hash
        self.arguments_json = hash.to_json
      end

      def arguments
        return nil unless self.arguments_json && self.arguments_json != 'null'
        @arguments ||= JSON.parse(self.arguments_json)
      rescue TypeError, LoadError, NameError, ArgumentError, JSON::ParserError => e
        raise DeserializationError,
          "Job failed to load: #{e.message}. Handler: #{handler.inspect}, Arguments: #{arguments_json}"
      end

      def handler_object
        @handler_object ||= self.handler.constantize
      rescue TypeError, LoadError, NameError, ArgumentError => e
        raise DeserializationError, "Job failed to load: #{e.message}. Handler: #{handler.inspect}, Arguments: #{arguments_json}"
      end

      def handler_object=(_handler)
        self.handler = _handler.name
      end

      def invoke_job
        hook :before
        handler_object.call(self.arguments)
        hook :success
      rescue Exception => e
        hook :error, e
        raise e
      ensure
        hook :after
      end

      # Unlock this job (note: not saved to DB)
      def unlock
        self.locked_at    = nil
        self.locked_by    = nil
      end

      def hook(name, *args)
        if handler_object.respond_to?(name)
          method = handler_object.method(name)
          method.arity == 0 ? method.call : method.call(self, *args)
        end
      rescue DeserializationError
        # do nothing
      end

      def reschedule_at
        if handler_object.respond_to?(:reschedule_at) 
          handler_object.reschedule_at(self.class.db_time_now, attempts) 
        else
          self.class.db_time_now + (attempts ** 4) + 5
        end
      end

      def max_attempts
        handler_object.max_attempts if handler_object.respond_to?(:max_attempts)
      end

      protected

      def set_default_run_at
        self.run_at ||= self.class.db_time_now
      end
    end
  end
end
