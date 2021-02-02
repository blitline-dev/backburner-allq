require 'base64'
require 'timeout'
require 'allq_rest'

module Backburner
  class AllqWatcher
    attr_accessor :tube

    def initialize(tube, allq_wrapper)
      @tube = tube
      @allq_wrapper = allq_wrapper
    end

    def watch
      Thread.new do
        ran = false
        job = @allq_wrapper.get(@tube_name)
        if job.body
          perform(job)
          ran = true
        end
        # Wait if nothing returned
        sleep(rand() * 3) unless ran
      end
    end
  end

  class AllQJob
    attr_accessor :id

    # Body
    attr_accessor :body

    # Tube name
    attr_accessor :tube

    # Expired count
    attr_accessor :expireds

    # Release count
    attr_accessor :releases

    def initialize(wrapper, job_resposne)
      @client = wrapper
      @id = job_resposne.id
      @body = job_resposne.body
      @expireds = job_resposne.expireds
      @releases = job_resposne.releases
      @tube = job_resposne.tube.to_s
    end

    def done
      @client.done(self)
    end

    def delete
      @client.delete(self)
    end

    def touch
      @client.touch(self)
    end

    def kick
      @client.kick(self)
    end

    def release(delay = 0)
      @client.release(self, delay)
    end

    def bury
      @client.bury(self)
    end

    def stats
      { 'expireds' => expireds, 'releases' => releases }
    end
  end

  class AllQWrapper
    def initialize(url = 'localhost:8090')
      allq_conf = Allq::Configuration.new do |config|
        config.host = url
      end

      raw_client = Allq::ApiClient.new(allq_conf)
      @client = Allq::ActionsApi.new(raw_client)
      @admin = Allq::AdminApi.new(raw_client)
      @recent_times = []

    end

    def speed
      if @recent_times.size > 0
        return @recent_times.sum(0.0) /  @recent_times.size
      end
      return 0
    end

    def touch(job)
      @client.touch_put(job.id)
    end

    def done(job)
      @client.job_delete(job.id)
    end

    def delete(job)
      @client.job_delete(job.id)
    end

    def release(job, _delay = 0)
      @client.release_put(job.id)
    end

    def bury(job)
     @client.bury_put(job.id)
    end

    def tube_names
      stats_hash = stats
      stats_hash.keys
    end
    alias_method :tubes, :tube_names


    def peek_buried(tube_name = 'default')
      job = nil
      job = @client.peek_get(tube_name, buried: true)
      return nil if job.body.nil?

      job.body = Base64.decode64(job.body) if job
      job_obj = Backburner::AllQJob.new(self, job)
      job_obj
    end

    def get(tube_name = 'default')
      job = nil
      job = @client.job_get(tube_name)

      # Inplace decode
      job.body = Base64.decode64(job.body) if job&.body

      job_obj = Backburner::AllQJob.new(self, job)
      job_obj
    rescue StandardError => ex
      if ex.message == "Couldn't resolve host name"
        puts("COUDNT RESOLVE HOST NAME------ SHOULD REBOOT")
      else
        puts(ex)
      end
    end

    def close
    rescue StandardError => ex
      puts(ex)
    end

    def map_priority(app_priority)
      app_priority = app_priority.to_i

      # IF already using allq-like priority, stick with it
      return app_priority if app_priority < 11 && app_priority > 0

      default = 5

      # return app_priority unless larger than 10
      app_priority > 10 ? 5 : app_priority
    end

    def log_result(job_result)
      puts("ALLQ-HTTP-JOB-ID=#{job_result.job_id}")
    rescue StandardError => ex
      puts(ex)
    end

    def build_new_job(body, options)
      adjusted_priority = map_priority(options[:pri] || 5)

      ttl = options[:ttl] || 600
      tube_name = options[:tube_name] || 'default'
      delay = options[:delay] || 0
      parent_id = options[:parent_id]

      new_job = Allq::NewJob.new(tube: tube_name,
                                 body: Base64.strict_encode64(body),
                                 ttl: ttl,
                                 delay: delay,
                                 priority: adjusted_priority,
                                 shard_key: options[:shard_key],
                                 parent_id: parent_id)
      new_job
    end

    def build_new_parent_job(body, options)
      adjusted_priority = map_priority(options[:pri] || 5)
      ttl = options[:ttl] || 600
      tube_name = options[:tube_name] || 'default'
      delay = options[:delay] || 0
      parent_id = options[:parent_id]
      limit = options[:limit]
      timeout = options[:timeout] || 3_600
      run_on_timeout = options[:run_on_timeout] || false

      new_parent_job = Allq::NewParentJob.new(tube: tube_name,
                                              body: Base64.strict_encode64(body),
                                              ttl: ttl,
                                              delay: delay,
                                              priority: adjusted_priority,
                                              timeout: timeout,
                                              run_on_timeout: run_on_timeout,
                                              shard_key: options[:shard_key],
                                              limit: limit)
      new_parent_job
    end

    def put(body, options)
      # New school put
      retry_count = 0
      is_parent = options[:is_parent] || false
      result = nil

      begin
        Timeout.timeout(10) do
          if body && body.to_s.include?('["default"]')
          end

          if is_parent
            new_job = build_new_parent_job(body, options)
            result = @client.parent_job_post(new_job)
          else
            new_job = build_new_job(body, options)
            result = @client.job_post(new_job)
          end
          raise 'PUT returned nil' if result.nil? || result.to_s == ''
        end
      rescue Timeout::Error
        puts('ALLQ_PUT_TIMEOUT')
        sleep(5)
        retry_count += 1
        retry if retry_count < 4
        raise 'Failed to put on allq, we are investigating the problem, please try again'
      rescue StandardError => ex
        puts('Failed to ALLQ PUT')
        puts(ex)
        retry_count += 1
        sleep(5)
        retry if retry_count < 4
        raise 'Failed to put on allq, we are investigating the problem, please try again'
      end
      result
    end

    def stats(tube)
      final_stats = stats
      final_stats[tube]
    end

    def stats
      raw_stats = @admin.stats_get
      final_stats = {}

      raw_stats.each do |agg|
        agg.stats.each do |tube_ref|
          name = tube_ref.tube
          final_stats[name] = {} unless final_stats[name]
          final_stats[name]['ready'] = final_stats[name]['ready'].to_i + tube_ref.ready.to_i
          final_stats[name]['reserved'] = final_stats[name]['reserved'].to_i + tube_ref.reserved.to_i
          final_stats[name]['delayed'] = final_stats[name]['delayed'].to_i + tube_ref.delayed.to_i
          final_stats[name]['buried'] = final_stats[name]['buried'].to_i + tube_ref.buried.to_i
          final_stats[name]['parents'] = final_stats[name]['parents'].to_i + tube_ref.parents.to_i
        end
      end
      final_stats
    rescue StandardError => ex
      puts(ex)
      {}
    end

    def beanstalk_style_stats
    end



    def get_ready_by_tube(name)
      count = -1
      tube_stats = stats[name]
      count = tube_stats['ready'].to_i if tube_stats && tube_stats['ready']
      count
    rescue StandardError => ex
      puts(ex)
      -1
    end

    def size
      result = get_ready_by_tube('default')
      result.to_i
    rescue StandardError => ex
      puts(ex)
      0
    end
  end
end
