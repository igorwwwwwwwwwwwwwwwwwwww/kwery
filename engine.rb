$: << 'lib'

require 'kwery'
require 'csv'

schema = Kwery::Schema.new
schema.column :id, :integer
schema.column :name, :string
schema.column :active, :boolean
schema.index :users_idx_id, [:users, :id, :asc], [:users, :name, :desc]

users = []
users_idx_id = Kwery::Index.new

csv = CSV.table('users.csv', converters: nil)
csv.each do |row|
  tup = schema.tuple(row)
  users << tup
  users_idx_id.insert(tup[:id], users.size-1)
end

context = {}
context[:users] = users
context[:users_idx_id] = users_idx_id

# SELECT name
# FROM users
# WHERE active = true
# ORDER BY id DESC
# LIMIT 10

query = Kwery::Query.new(
  select: {
    id: Kwery::Query::Field.new(:users, :id),
    name: Kwery::Query::Field.new(:users, :name),
  },
  from: :users,
  where: Kwery::Query::Eq.new(Kwery::Query::Field.new(:users, :active), Kwery::Query::Literal.new(true)),
  order: [
    Kwery::Query::OrderedField.new(Kwery::Query::Field.new(:users, :id), :desc),
    Kwery::Query::OrderedField.new(Kwery::Query::Field.new(:users, :name), :asc),
  ],
  limit: 10,
)

plan = query.plan(schema)

mode = ARGV.shift || 'run'
case mode
when 'query'
  pp query
when 'plan'
  pp plan
when 'run'
  plan.call(context).each do |tup|
    puts tup
  end
else
  raise "invalid mode #{mode}"
end
