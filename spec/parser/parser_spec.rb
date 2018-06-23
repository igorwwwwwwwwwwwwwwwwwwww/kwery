require 'kwery'

RSpec.describe Kwery::Parser::Lexer do
  it "parses select num" do
    sql = 'SELECT 1'
    parser = Kwery::Parser::Parser.new
    expect(parser.parse(sql)).to eq(
      Kwery::Query.new(select: { _0: Kwery::Expr::Literal.new(1) })
    )
  end

  it "parses lower case select" do
    sql = 'select 1'
    parser = Kwery::Parser::Parser.new
    expect(parser.parse(sql)).to eq(
      Kwery::Query.new(select: { _0: Kwery::Expr::Literal.new(1) })
    )
  end

  it "parses select num+" do
    sql = 'SELECT 64'
    parser = Kwery::Parser::Parser.new
    expect(parser.parse(sql)).to eq(
      Kwery::Query.new(select: { _0: Kwery::Expr::Literal.new(64) })
    )
  end

  it "parses select str" do
    sql = "SELECT 'foo'"
    parser = Kwery::Parser::Parser.new
    expect(parser.parse(sql)).to eq(
      Kwery::Query.new(select: { _0: Kwery::Expr::Literal.new('foo') })
    )
  end

  it "parses select empty str" do
    sql = "SELECT ''"
    parser = Kwery::Parser::Parser.new
    expect(parser.parse(sql)).to eq(
      Kwery::Query.new(select: { _0: Kwery::Expr::Literal.new('') })
    )
  end

  it "parses select escaped str" do
    sql = "SELECT '\\''"
    parser = Kwery::Parser::Parser.new
    expect(parser.parse(sql)).to eq(
      Kwery::Query.new(select: { _0: Kwery::Expr::Literal.new("'") })
    )
  end

  it "parses select bool" do
    sql = 'SELECT true'
    parser = Kwery::Parser::Parser.new
    expect(parser.parse(sql)).to eq(
      Kwery::Query.new(select: { _0: Kwery::Expr::Literal.new(true) })
    )
  end

  it "parses short bool" do
    sql = 'SELECT t'
    parser = Kwery::Parser::Parser.new
    expect(parser.parse(sql)).to eq(
      Kwery::Query.new(select: { _0: Kwery::Expr::Literal.new(true) })
    )
  end

  it "parses select name from users" do
    sql = "SELECT name FROM users"
    parser = Kwery::Parser::Parser.new
    expect(parser.parse(sql)).to eq(
      Kwery::Query.new(
        select: { name: Kwery::Expr::Column.new(:name) },
        from: :users,
      )
    )
  end

  it "parses select name from users where id = 1" do
    sql = "SELECT name FROM users WHERE id = 1"
    parser = Kwery::Parser::Parser.new
    expect(parser.parse(sql)).to eq(
      Kwery::Query.new(
        select: { name: Kwery::Expr::Column.new(:name) },
        from: :users,
        where: Kwery::Expr::Eq.new(
          Kwery::Expr::Column.new(:id),
          Kwery::Expr::Literal.new(1),
        ),
      )
    )
  end
end
