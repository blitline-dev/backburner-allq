require 'delegate'

module Backburner
  class Connection
    class BadURL < RuntimeError; end

    attr_accessor :url, :allq_wrapper

    # If a proc is provided, it will be called (and given this connection as an
    # argument) whenever the connection is reconnected.
    # @example
    #   connection.on_reconnect = lambda { |conn| puts 'reconnected!' }
    attr_accessor :on_reconnect

    # Constructs a backburner connection
    # `url` can be a string i.e '127.0.0.1:3001' or an array of
    # addresses (however, only the first element in the array will
    # be used)
    def initialize(url, &on_reconnect)
      @url = url
      @allq_wrapper = nil
      @on_reconnect = on_reconnect
      connect!
    end

    # Close the connection, if it exists
    def close
      @allq_wrapper.close if @allq_wrapper
      @allq_wrapper = nil
    end

    # Determines if the connection to allq is currently open
    def connected?
      begin
        !!(@allq_wrapper && @allq_wrapper.connection && @allq_wrapper.connection.connection && !@allq_wrapper.connection.connection.closed?) # Would be nice if beaneater provided a connected? method
      rescue
        false
      end
    end

    # Attempt to reconnect to allq. Note: the connection will not be watching
    # or using the tubes it was before it was reconnected (as it's actually a
    # completely new connection)
    # @raise [Beaneater::NotConnected] If allq fails to connect
    def reconnect!
      close
      connect!
      @on_reconnect.call(self) if @on_reconnect.respond_to?(:call)
    end

    # Yield to a block that will be retried several times if the connection to
    # allq goes down and is able to be re-established.
    #
    # @param options Hash Options. Valid options are:
    #   :max_retries       Integer The maximum number of times the block will be yielded to.
    #                              Defaults to 4
    #   :on_retry          Proc    An optional proc that will be called for each retry. Will be
    #                              called after the connection is re-established and :retry_delay
    #                              has passed but before the block is yielded to again
    #   :retry_delay       Float   The amount to sleep before retrying. Defaults to 1.0
    # @raise Beaneater::NotConnected If a connection is unable to be re-established
    def retryable(options = {}, &block)
      options = {:max_retries => 4, :on_retry => nil, :retry_delay => 1.0}.merge!(options)
      retry_count = options[:max_retries]

      begin
        yield

      rescue Exception => e
        if retry_count > 0
          reconnect!
          retry_count -= 1
          sleep options[:retry_delay]
          options[:on_retry].call if options[:on_retry].respond_to?(:call)
          retry
        else # stop retrying
          raise e
        end
      end
    end

    # Attempt to ensure we're connected to allq if the missing method is
    # present in the delegate and we haven't shut down the connection on purpose
    # @raise [Beaneater::NotConnected] If allq fails to connect after multiple attempts.
    def method_missing(m, *args, &block)
      ensure_connected! if respond_to_missing?(m, false)
      super
    end

    # Connects to a allq queue
    # @raise Beaneater::NotConnected if the connection cannot be established
    def connect!
      @allq_wrapper = Backburner::AllQWrapper.new(allq_addresses)
      @allq_wrapper
    end

    def put(tube_name, data, opt)
      pri = (opt[:pri] || 5).to_i
      ttr = (opt[:ttr] || 600).to_i

      options = {
        tube_name: tube_name,
        pri: pri,
        delay: opt[:delay].to_i,
        ttr: ttr
      }

      options.merge!(opt)
      @allq_wrapper.put(data, options)
    end

    def get(tube_name)
      @allq_wrapper.get(tube_name)
    end

    # Attempts to ensure a connection to allq is established but only if
    # we're not connected already
    # @param max_retries Integer The maximum number of times to attempt connecting. Defaults to 4
    # @param retry_delay Float   The time to wait between retrying to connect. Defaults to 1.0
    # @raise [Beaneater::NotConnected] If allq fails to connect after multiple attempts.
    # @return Connection This Connection is returned if the connection to allq is open or was able to be reconnected
    def ensure_connected!(max_retries = 4, retry_delay = 1.0)
      return self if connected?

      begin
        reconnect!
        return self

      rescue Exception => e
        if max_retries > 0
          max_retries -= 1
          sleep retry_delay
          retry
        else # stop retrying
          raise e
        end
      end
    end

    # Returns the allq queue addresses
    #
    # @example
    #   allq_addresses => ["127.0.0.1:11300"]
    #
    def allq_addresses
      uri = self.url.is_a?(Array) ? self.url.first : self.url
      allq_host_and_port(uri)
    end

    # Returns a host and port based on the uri_string given
    #
    # @example
    #   allq_host_and_port("allq://127.0.0.1") => "127.0.0.1:11300"
    #
    def allq_host_and_port(uri_string)
      uri = URI.parse(uri_string)
      raise(BadURL, uri_string) if uri.scheme != 'allq'.freeze
      "#{uri.host}:#{uri.port || 11300}"
    end
  end # Connection
end # Backburner
