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

    def query(sql, client_opts = {}, context = nil)
      headers = {}
      headers['Partial'] = 'true' if client_opts[:partial]

      context.increment(:backend_requests) if context

      response = @conn.post('/query', sql, headers)
      JSON.parse(response.body, symbolize_names: true)
    end

    def insert(table, data, context = nil)
      context.increment(:backend_requests) if context

      response = @conn.post("/insert/#{table}", data.to_json)
      JSON.parse(response.body, symbolize_names: true)
    end

    class Batch
      def query(queries, client_opts = {}, context = nil)
        hydra = Typhoeus::Hydra.new

        headers = {}
        headers['Partial'] = 'true' if client_opts[:partial]

        reqs = queries.map do |backend, sql|
          Typhoeus::Request.new(
            "#{backend}/query",
            method: :post,
            body: sql,
            headers: headers,
          )
        end

        reqs.each do |req|
          context.increment(:backend_requests) if context
          hydra.queue(req)
        end
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
