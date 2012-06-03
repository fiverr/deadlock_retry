module DeadlockRetry
  def self.included(base)
    base.extend(ClassMethods)
    base.class_eval do
      class << self
        alias_method_chain :transaction, :deadlock_handling
      end
    end
  end

  module ClassMethods
    DEADLOCK_ERROR_MESSAGES = [
      "Deadlock found when trying to get lock",
      "Lock wait timeout exceeded",
      "deadlock detected"
    ]

    MAXIMUM_RETRIES_ON_DEADLOCK = 3

    def transaction_with_deadlock_handling(*objects, &block)
      retry_count = 0
      begin
        transaction_without_deadlock_handling(*objects, &block)
      rescue ActiveRecord::StatementInvalid => error
        raise if in_nested_transaction?
        if DEADLOCK_ERROR_MESSAGES.any? { |msg| error.message =~ /#{Regexp.escape(msg)}/ }
          raise if retry_count >= self.class.transaction_lock_retries
          retry_count += 1
          logger.info "Deadlock detected on retry #{retry_count}, restarting transaction"
          exponential_pause(retry_count)
          retry
        else
          raise
        end
      end
    end

    private

    WAIT_TIMES = [0, 1, 2, 3, 4, 5, 6]

    def exponential_pause(count)
      sec = WAIT_TIMES[count-1] || 32
      # sleep 0, 1, 2, 4, ... seconds up to the MAXIMUM_RETRIES.
      # Cap the pause time at 32 seconds.
      sleep(sec) if sec != 0
    end

    def in_nested_transaction?
      # open_transactions was added in 2.2's connection pooling changes.
      connection.open_transactions != 0
    end

  end
end

class ActiveRecord::Base
  
  cattr_reader :maximum_transaction_lock_retries
  
  def self.transaction_lock_retries
    @@maximum_transaction_lock_retries || 3
  end
  
  def self.transaction_lock_retries=(maximum)
    if maximum < 0
      maximum = 0
    end
    @@maximum_transaction_lock_retries = maximum
  end
end
ActiveRecord::Base.send(:include, DeadlockRetry) if defined?(ActiveRecord)
