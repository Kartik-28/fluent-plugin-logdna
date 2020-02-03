require 'fluent/output'
require 'net/https'
require 'httpclient'

module Fluent

  class LogDNAConnection

    attr_reader :http
  
    def initialize(proxy_uri)
      create_http_client(proxy_uri)
    end
  
    def publish(url,raw_data,api_key,content_type)
      response = http.post(url, raw_data, request_headers(api_key,content_type))
      unless response.ok?
        raise RuntimeError, "Failed to send data to HTTP Source. #{response.code} - #{response.body}"
      end
    end
  
    def request_headers(api_key,content_type)
      headers = {
          'apikey'     => api_key,
          'content-type' => content_type
      }
      return headers
    end
  
    def create_http_client(proxy_uri)
      @http                        = HTTPClient.new(proxy_uri)
      
    end
  end

  class LogDNAOutput < Fluent::BufferedOutput
    Fluent::Plugin.register_output('logdna', self)

    MAX_RETRIES = 5

    config_param :api_key, :string, secret: true
    config_param :hostname, :string
    config_param :mac, :string, default: nil
    config_param :ip, :string, default: nil
    config_param :app, :string, default: nil
    config_param :file, :string, default: nil
    config_param :proxy_uri, :string, :default => nil
    config_param :ingester_domain, :string, default: 'https://logs.logdna.com'

    def configure(conf)
      super
      @host = conf['hostname']
      @logDNA_conn = LogDNAConnection.new(conf['proxy_uri'])
      
    end

    def start
      super
      require 'json'
      require 'base64'
      require 'http'
      HTTP.default_options = { :keep_alive_timeout => 60 }
      @ingester = HTTP.persistent @ingester_domain
      @requests = Queue.new
      
    end

    def shutdown
      super
      @ingester.close if @ingester
    end

    def format(tag, time, record)
      [tag, time, record].to_msgpack
    end

    def write(chunk)
      body = chunk_to_body(chunk)
      response = send_request(body)
      raise 'Encountered server error' if response.code >= 400
      response.flush
    end

    private

    def chunk_to_body(chunk)
      data = []

      chunk.msgpack_each do |(tag, time, record)|
        line = gather_line_data(tag, time, record)
        data << line unless line[:line].empty?
      end

      { lines: data }
    end

    def gather_line_data(tag, time, record)
      line = {
        level: record['level'] || record['severity'] || tag.split('.').last,
        timestamp: time,
        line: record.to_json
      }
      # At least one of "file" or "app" is required.
      line[:file] = record['file']
      line[:file] ||= @file if @file
      line.delete(:file) if line[:file].nil?
      line[:app] = record['_app'] || record['app']
      line[:app] ||= @app if @app
      line.delete(:app) if line[:app].nil?
      line[:env] = record['env']
      line.delete(:env) if line[:env].nil?
      line[:meta] = record['meta']
      line.delete(:meta) if line[:meta].nil?
      line
    end

    def send_request(body)
      now = Time.now.to_i
      url = "/logs/ingest?hostname=#{@host}&mac=#{@mac}&ip=#{@ip}&now=#{now}"
      
      @logDNA_conn.publish(url,body,@api_key,'application/json')
    end
  end
end
