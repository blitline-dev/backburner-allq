module Backburner
  class Configuration
    PRIORITY_LABELS = { :high => 0, :medium => 5, :low => 9 }

    attr_accessor :allq_url       # beanstalk url connection
    attr_accessor :tube_namespace      # namespace prefix for every queue
    attr_reader   :namespace_separator # namespace separator
    attr_accessor :default_priority    # default job priority
    attr_accessor :respond_timeout     # default job timeout
    attr_accessor :on_error            # error handler
    attr_accessor :max_job_retries     # max job retries
    attr_accessor :retry_delay         # (minimum) retry delay in seconds
    attr_accessor :retry_delay_proc    # proc to calculate delay (and allow for back-off)
    attr_accessor :default_queues      # default queues
    attr_accessor :logger              # logger
    attr_accessor :default_worker      # default worker class
    attr_accessor :primary_queue       # the general queue
    attr_accessor :priority_labels     # priority labels
    attr_accessor :reserve_timeout     # duration to wait to reserve on a single server
    attr_accessor :job_serializer_proc # proc to write the job body to a string
    attr_accessor :job_parser_proc     # proc to parse a job body from a string

    def initialize
      @allq_url       = "allq://127.0.0.1:8091"
      @tube_namespace      = "backburner"
      @namespace_separator = "."
      @default_priority    = 5
      @respond_timeout     = 120
      @on_error            = nil
      @max_job_retries     = 1
      @retry_delay         = 5
      @retry_delay_proc    = lambda { |min_retry_delay, num_retries| min_retry_delay + (num_retries ** 3) }
      @default_queues      = []
      @logger              = nil
      @default_worker      = Backburner::Workers::Simple
      @primary_queue       = "backburner-jobs"
      @priority_labels     = PRIORITY_LABELS
      @reserve_timeout     = nil
      @job_serializer_proc = lambda { |body| body.to_json }
      @job_parser_proc     = lambda { |body| JSON.parse(body) }
    end

    def namespace_separator=(val)
      raise 'Namespace separator cannot used reserved queue configuration separator ":"' if val == ':'
      @namespace_separator = val
    end
  end # Configuration
end # Backburner
