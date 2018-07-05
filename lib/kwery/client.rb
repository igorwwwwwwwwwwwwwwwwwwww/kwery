require 'faraday'
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

    def query(sql)
      response = @conn.post('/query', sql)
      JSON.parse(response.body, symbolize_names: true)
    end

    def insert(table, data)
      response = @conn.post("/insert/#{table}", data.to_json)
      JSON.parse(response.body, symbolize_names: true)
    end
  end
end
