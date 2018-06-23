$: << 'lib'

require 'kwery'

catalog = Kwery::Catalog.new

catalog.table :users, Kwery::Catalog::Table.new(
  columns: {
    id:     Kwery::Catalog::Column.new(:integer),
    name:   Kwery::Catalog::Column.new(:string),
    active: Kwery::Catalog::Column.new(:boolean),
  },
  indexes: [:users_idx_id],
)
catalog.index :users_idx_id, Kwery::Catalog::Index.new(:users, [
  Kwery::Catalog::IndexedExpr.new(Kwery::Expr::Column.new(:id), :asc),
])

schema = catalog.new_schema

importer = Kwery::Importer.new(catalog, schema)
importer.load(:users, 'data/users.csv')

schema.reindex(:users, :users_idx_id)

mode = ARGV.shift
sql = ARGV.shift

unless mode && sql
  puts 'Usage:'
  puts '  ruby engine.rb run     <sql>'
  puts '  ruby engine.rb query   <sql>'
  puts '  ruby engine.rb explain <sql>'
  exit 1
end

parser = Kwery::Parser.new
query = parser.parse(sql, ENV['DEBUG'] == 'true')
query.options = { notablescan: ENV['NOTABLESCAN'] == 'true' }

plan = query.plan(catalog)

case mode
when 'query'
  pp query
when 'explain'
  pp plan.explain
when 'run'
  context = Kwery::Executor::Context.new(schema)
  plan.call(context).each do |tup|
    puts tup
  end
  puts
  puts "stats: #{context.stats}"
else
  raise "invalid mode #{mode}"
end
