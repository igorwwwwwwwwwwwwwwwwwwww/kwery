$: << 'lib'

$stdout.sync = true
$stderr.sync = true

if ENV['PROXY'] == 'true'
  require 'kwery/web/proxy'
elsif ENV['REPLICA'] == 'true'
  require 'kwery/web/replica'
else
  require 'kwery/web/server'
end

run Sinatra::Application
