require 'faraday'
require 'typhoeus'
require 'json'

module Kwery
  class Client
    def initialize(backend)
      @backend = backend
      @conn = Faraday.new(url: @backend) do |c|
        c.use Faraday::Response::RaiseError
        c.use Faraday::Adapter::NetHttp
      end
    end

    def query(sql, client_opts = {})
      headers = {}
      headers['Partial'] = 'true' if client_opts[:partial]

      response = @conn.post('/query', sql, headers)

      JSON.parse(response.body, symbolize_names: true)
    end

    def insert(table, data)
      response = @conn.post("/insert/#{table}", data.to_json)
      JSON.parse(response.body, symbolize_names: true)
    end

    class Batch
      def initialize(backends)
        @backends = backends
      end

      def query(sql, client_opts = {})
        hydra = Typhoeus::Hydra.new

        headers = {}
        headers['Partial'] = 'true' if client_opts[:partial]

        reqs = @backends.map do |backend|
          Typhoeus::Request.new(
            "#{backend}/query",
            method: :post,
            body: sql,
            headers: headers,
          )
        end

        reqs.each { |req| hydra.queue(req) }
        hydra.run

        reqs.map do |req|
          unless req.response.success?
            raise "unsuccessful response for request #{req.url} status=#{req.response.code} timeout=#{req.response.timed_out?}"
          end
          JSON.parse(req.response.body, symbolize_names: true)
        end
      end
    end
  end
end
