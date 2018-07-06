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
      headers = { 'Partial' => 'true' }
      response = @conn.post('/query', sql, headers)
      JSON.parse(response.body, symbolize_names: true)
    end

    def insert(table, data)
      headers = { 'Partial' => 'true' }
      response = @conn.post("/insert/#{table}", data.to_json, headers)
      JSON.parse(response.body, symbolize_names: true)
    end
  end
end
