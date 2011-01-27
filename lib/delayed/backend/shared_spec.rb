require File.expand_path('../../../../spec/sample_jobs', __FILE__)

shared_examples_for 'a delayed_job backend' do
  let(:worker) { Delayed::Worker.new }
  let(:now) { described_class.db_time_now }
  let(:later) { now + 5.minutes }

  def create_job(opts = {})
    described_class.create(opts.merge(:handler_object => SimpleJob, :arguments => {}))
  end

  before do
    Delayed::Worker.max_priority = nil
    Delayed::Worker.min_priority = nil
    Delayed::Worker.default_priority = 99
    Delayed::Worker.delay_jobs = true
    SimpleJob.runs = 0
    described_class.delete_all
  end

  it "should set run_at automatically if not set" do
    described_class.create(:arguments => ErrorJob.new ).run_at.should_not be_nil
  end

  it "should not set run_at automatically if already set" do
    job = described_class.create(:arguments => ErrorJob.new, :run_at => later)
    job.run_at.should be_within(1).of(later)
  end

  describe ".enqueue" do
    subject { job }
    let(:job) { described_class.enqueue(handler, arguments, options) }
    let(:handler) { SimpleJob }
    let(:arguments) { Hash.new }
    let(:options) { Hash.new }

    it "should use default priority" do
      job.priority.should == 99
    end

    it 'should set run_at by default' do
      job.run_at.should_not be_nil
    end

    context 'with handler that does not respond_to :call' do
      let(:handler) { Class.new }

      it "should raise ArgumentError" do
        lambda { job }.should raise_error(ArgumentError)
      end
    end

    context 'with a non-hash arugments' do
      let(:arguments) { Array.new }

      it "should raise ArgumentError" do
        lambda { job }.should raise_error(ArgumentError)
      end
    end

    context 'with priority' do
      let(:options) { { :priority => 5 } }

      it "should be able to set priority" do
        job.priority.should == 5
      end
    end

    context 'with run_at' do
      let(:options) { { :run_at => later } }

      it "should be able to set run_at" do
        job.run_at.should be_within(1).of(later)
      end
    end
  end

  context "when processing callbacks" do
    before(:each) do
      CallbackJob.messages = []
    end

    let(:job) { described_class.enqueue(CallbackJob) }

    %w(before success after).each do |callback|
      it "should call #{callback} with job" do
        job.handler_object.should_receive(callback).with(job)
        job.invoke_job
      end
    end

    it "should call before and after callbacks" do
      job
      CallbackJob.messages.should == ["enqueue"]
      job.invoke_job
      CallbackJob.messages.should == ["enqueue", "before", "call", "success", "after"]
    end

    it "should call the after callback with an error" do
      job.handler_object.should_receive(:call).and_raise(RuntimeError.new("fail"))

      lambda { job.invoke_job }.should raise_error
      CallbackJob.messages.should == ["enqueue", "before", "error: RuntimeError", "after"]
    end

    it "should call error when before raises an error" do
      job.handler_object.should_receive(:before).and_raise(RuntimeError.new("fail"))
      lambda { job.invoke_job }.should raise_error(RuntimeError)
      CallbackJob.messages.should == ["enqueue", "error: RuntimeError", "after"]
    end
  end

  describe ".reserve" do
    before do
      Delayed::Worker.max_run_time = 2.minutes
    end

    it "should not reserve failed jobs" do
      create_job :attempts => 50, :failed_at => described_class.db_time_now
      described_class.reserve(worker).should be_nil
    end

    it "should not reserve jobs scheduled for the future" do
      create_job :run_at => described_class.db_time_now + 1.minute
      described_class.reserve(worker).should be_nil
    end

    it "should reserve jobs scheduled for the past" do
      job = create_job :run_at => described_class.db_time_now - 1.minute
      described_class.reserve(worker).should == job
    end

    it "should reserve jobs scheduled for the past when time zones are involved" do
      Time.zone = 'US/Eastern'
      job = create_job :run_at => described_class.db_time_now - 1.minute.ago.in_time_zone
      described_class.reserve(worker).should == job
    end

    it "should not reserve jobs locked by other workers" do
      job = create_job
      other_worker = Delayed::Worker.new
      other_worker.name = 'other_worker'
      described_class.reserve(other_worker).should == job
      described_class.reserve(worker).should be_nil
    end

    it "should reserve open jobs" do
      job = create_job
      described_class.reserve(worker).should == job
    end

    it "should reserve expired jobs" do
      job = create_job(:locked_by => worker.name, :locked_at => described_class.db_time_now - 3.minutes)
      described_class.reserve(worker).should == job
    end

    it "should reserve own jobs" do
      job = create_job(:locked_by => worker.name, :locked_at => (described_class.db_time_now - 1.minutes))
      described_class.reserve(worker).should == job
    end
  end

  describe "#name" do
    it "should be the class name of the job that was enqueued" do
      described_class.create(:handler_object => ErrorJob).name.should eql('ErrorJob')
    end

    it "should be the method that will be called if its a performable method object" do
      described_class.new(:handler_object => NamedJob).name.should eql('named_job')
    end

    pending "should be the instance method that will be called if its a performable method object" do
      @job = Story.create(:text => "...").delay.save
      @job.name.should == 'Story#save'
    end

    pending "should parse from handler on deserialization error" do
      job = Story.create(:text => "...").delay.text
      job.arguments.object.destroy
      job = described_class.find(job.id)
      job.name.should == 'Delayed::PerformableMethod'
    end
  end

  context "when prioritizing workers" do
    before(:each) do
      Delayed::Worker.max_priority = nil
      Delayed::Worker.min_priority = nil
    end

    it "should fetch jobs ordered by priority" do
      10.times { described_class.enqueue SimpleJob, :priority => rand(10) }
      jobs = []
      10.times { jobs << described_class.reserve(worker) }
      jobs.size.should == 10
      jobs.each_cons(2) do |a, b|
        a.priority.should <= b.priority
      end
    end

    it "should only find jobs greater than or equal to min priority" do
      min = 5
      Delayed::Worker.min_priority = min
      10.times {|i| described_class.enqueue SimpleJob, :priority => i }
      5.times { described_class.reserve(worker).priority.should >= min }
    end

    it "should only find jobs less than or equal to max priority" do
      max = 5
      Delayed::Worker.max_priority = max
      10.times {|i| described_class.enqueue SimpleJob, {}, :priority => i }
      5.times { described_class.reserve(worker).priority.should <= max }
    end
  end

  # This isn't in base
  describe ".clear_locks!" do
    let(:job) { create_job(:locked_by => 'worker1', :locked_at => now) }

    it "should clear locks for the given worker" do
      job.should_not be_new_record
      described_class.clear_locks!('worker1')
      described_class.reserve(worker).should == job
    end

    it "should not clear locks for other workers" do
      job.should_not be_new_record
      described_class.clear_locks!('different_worker')
      described_class.reserve(worker).should_not == job
    end
  end

  context ".unlock" do
    let(:job) { create_job(:locked_by => 'worker', :locked_at => described_class.db_time_now) }

    it "should clear locks" do
      job.unlock
      job.locked_by.should be_nil
      job.locked_at.should be_nil
    end
  end

  pending "large handler" do
    let(:arguments) { "Lorem ipsum dolor sit amet. " * 1000 }
    let(:job) {  described_class.enqueue Delayed::PerformableMethod.new(text, :length, {}) }

    it "should have an id" do
      job.id.should_not be_nil
    end
  end
  
  context "max_attempts" do
    let(:job) { described_class.enqueue SimpleJob }
    
    it 'should not be defined' do
      job.max_attempts.should be_nil
    end
    
    it 'should use the max_retries value on the payload when defined' do
      job.handler_object.stub!(:max_attempts).and_return(99)
      job.max_attempts.should eql(99)
    end 
  end

  context "when integrating with worker" do
    before do
      Delayed::Job.delete_all
      SimpleJob.runs = 0
    end

    context "when running a job" do
      let(:job) { Delayed::Job.create :handler_object => LongRunningJob }

      it "should fail after Worker.max_run_time" do
        begin
          old_max_run_time = Delayed::Worker.max_run_time
          Delayed::Worker.max_run_time = 1.second
          worker.run(job)
          job.reload.last_error.should =~ /expired/
          job.attempts.should == 1
        ensure
          Delayed::Worker.max_run_time = old_max_run_time
        end
      end

      context "when the job raises a deserialization error" do
        it "should mark the job as failed" do
          Delayed::Worker.destroy_failed_jobs = false
          job = described_class.create! :handler => "JobThatDoesNotExist", :arguments => {}
          worker.work_off
          job.reload
          job.failed_at.should_not be_nil
        end
      end
    end

    context "when job fails" do
      before do
        # reset defaults
        Delayed::Worker.destroy_failed_jobs = true
        Delayed::Worker.max_attempts = 25
      end

      let(:job) { Delayed::Job.enqueue(handler) }
      let(:handler) { ErrorJob }

      it "should record last_error when destroy_failed_jobs = false, max_attempts = 1" do
        Delayed::Worker.destroy_failed_jobs = false
        Delayed::Worker.max_attempts = 1
        worker.run(job)
        job.reload
        job.last_error.should =~ /did not work/
        job.attempts.should == 1
        job.failed_at.should_not be_nil
      end

      it "should re-schedule jobs after failing" do
        worker.run(job)
        worker.work_off
        job.reload
        job.last_error.should =~ /did not work/
        job.last_error.should =~ /sample_jobs.rb:\d+:in `call'/
        job.attempts.should == 1
        job.run_at.should > Delayed::Job.db_time_now - 10.minutes
        job.run_at.should < Delayed::Job.db_time_now + 10.minutes
        job.locked_by.should be_nil
        job.locked_at.should be_nil
      end

      context 'with handler provided time' do
        let(:handler) { CustomRescheduleJob.tap { |c| c.offset = 99.minutes }  }
        it 'should re-schedule with handler provided time if present' do
          worker.run(job)
          job.reload

          (Delayed::Job.db_time_now + 99.minutes - job.run_at).abs.should < 1
        end
      end

      it "should not fail when the triggered error doesn't have a message" do
        error_with_nil_message = StandardError.new
        error_with_nil_message.stub!(:message).and_return nil
        job.stub!(:invoke_job).and_raise error_with_nil_message
        lambda{worker.run(job)}.should_not raise_error
      end
    end

    context "when rescheduleing" do
      let(:job) { Delayed::Job.create :handler_object => handler_object }
      let(:handler_object) { SimpleJob }

      share_examples_for "any failure more than Worker.max_attempts times" do
        context "when the job's payload has a #failure hook" do
          let(:handler_object) { OnPermanentFailureJob }

          it "should run that hook" do
            job.handler_object.should respond_to :failure
            job.handler_object.should_receive :failure
            worker.reschedule(job)
          end
        end

        context "when the job's payload has no #failure hook" do
          # It's a little tricky to test this in a straightforward way,
          # because putting a should_not_receive expectation on
          # @job.arguments.failure makes that object
          # incorrectly return true to
          # arguments.respond_to? :failure, which is what
          # reschedule uses to decide whether to call failure.
          # So instead, we just make sure that the arguments as it
          # already stands doesn't respond_to? failure, then
          # shove it through the iterated reschedule loop and make sure we
          # don't get a NoMethodError (caused by calling that nonexistent
          # failure method).

          it "should not try to run that hook" do
            job.handler_object.should_not respond_to(:failure)
            lambda do
              Delayed::Worker.max_attempts.times { worker.reschedule(job) }
            end.should_not raise_exception(NoMethodError)
          end
        end
      end

      context "and we want to destroy jobs" do
        before do
          Delayed::Worker.destroy_failed_jobs = true
        end

        it_should_behave_like "any failure more than Worker.max_attempts times"

        it "should be destroyed if it failed more than Worker.max_attempts times" do
          job.should_receive(:destroy)
          Delayed::Worker.max_attempts.times { worker.reschedule(job) }
        end

        it "should not be destroyed if failed fewer than Worker.max_attempts times" do
          job.should_not_receive(:destroy)
          (Delayed::Worker.max_attempts - 1).times { worker.reschedule(job) }
        end
      end

      context "and we don't want to destroy jobs" do
        before do
          Delayed::Worker.destroy_failed_jobs = false
        end

        it_should_behave_like "any failure more than Worker.max_attempts times"

        it "should be failed if it failed more than Worker.max_attempts times" do
          job.reload.failed_at.should == nil
          Delayed::Worker.max_attempts.times { worker.reschedule(job) }
          job.reload.failed_at.should_not == nil
        end

        it "should not be failed if it failed fewer than Worker.max_attempts times" do
          (Delayed::Worker.max_attempts - 1).times { worker.reschedule(job) }
          job.reload.failed_at.should == nil
        end
      end
    end
  end
end
