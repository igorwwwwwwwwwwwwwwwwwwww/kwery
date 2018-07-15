require 'typhoeus'
require 'json'

# TODO: timeouts

module Kwery
  module Client
    class Batch
      def raft_leader(nodes, client_opts = {}, context = nil)
        reqs = nodes.map do |node|
          Typhoeus::Request.new("#{node}/raft/leader")
        end

        hydra = Typhoeus::Hydra.new
        reqs.each do |req|
          context.increment(:backend_requests) if context
          hydra.queue(req)
        end
        hydra.run

        reqs
          .map { |req|
            unless req.response.success?
              error = extract_error(req.response)
              warn "error response for request #{req.url} status=#{req.response.code} timeout=#{req.response.timed_out?} body=\"#{req.options[:body]}\" error=\"#{error}\""
              next
            end
            JSON.parse(req.response.body, symbolize_names: true)
          }
          .reject { |x| x.nil? }
      end

      def query(queries, client_opts = {}, context = nil)
        headers = {}
        headers['Partial'] = 'true' if client_opts[:partial]

        # body can be sql or { query: sql, data: stdin }
        reqs = queries.map do |backend, body|
          Typhoeus::Request.new(
            "#{backend}/query",
            method: :post,
            body: body,
            headers: headers,
            verbose: ENV['DEBUG_TYPHOEUS'] == 'true',
          )
        end

        hydra = Typhoeus::Hydra.new
        reqs.each do |req|
          context.increment(:backend_requests) if context
          hydra.queue(req)
        end
        hydra.run

        reqs.map do |req|
          unless req.response.success?
            # TODO expose nested exception metadata in structured form
            error = extract_error(req.response)
            raise "error response for request #{req.url} status=#{req.response.code} timeout=#{req.response.timed_out?} body=\"#{req.options[:body]}\" error=\"#{error}\""
          end
          JSON.parse(req.response.body, symbolize_names: true)
        end
      end

      def extract_error(response)
        return unless response.body
        begin
          result = JSON.parse(response.body, symbolize_names:true)
          result[:error]
        rescue
          nil
        end
      end
    end
  end
end
