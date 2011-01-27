class NamedJob < Struct.new(:call)
  def self.display_name
    'named_job'
  end
end

class SimpleJob
  cattr_accessor :runs; self.runs = 0
  def self.call(args); @@runs += 1; end
end

class ErrorJob
  cattr_accessor :runs; self.runs = 0
  def self.call(args)
    raise 'did not work'
  end
end

class CustomRescheduleJob 
  cattr_accessor :runs, :offset; self.runs = 0
  def self.call(args); raise 'did not work'; end
  def self.reschedule_at(time, attempts); time + offset; end
end

class LongRunningJob
  def self.call(args)
    sleep 250
  end
end

class OnPermanentFailureJob < SimpleJob
  def self.failure; end
  def self.max_attempts; 1; end
end

module M
  class ModuleJob
    cattr_accessor :runs; self.runs = 0
    def self.call(args); @@runs += 1; end
  end
end

class CallbackJob
  cattr_accessor :messages
  

  def self.enqueue(job)
    messages << 'enqueue'
  end

  def self.before(job)
    messages << 'before'
  end

  def self.call(args)
    messages << 'call'
  end

  def self.after(job)
    messages << 'after'
  end

  def self.success(job)
    messages << 'success'
  end

  def self.error(job, error)
    messages << "error: #{error.class}"
  end

  def self.failure(job)
    messages << 'failure'
  end
end
