require 'kwery'

RSpec.describe Kwery::Parser::Lexer do
  it "lexes select num" do
    tokens =
    sql = 'SELECT 1'
    lex = Kwery::Parser::Lexer.new(sql)
    expect(lex.pairs).to eq([
      [:SELECT, 'SELECT'],
      [:NUMBER, 1],
    ])
  end

  it "lexes select num+" do
    tokens =
    sql = 'SELECT 64'
    lex = Kwery::Parser::Lexer.new(sql)
    expect(lex.pairs).to eq([
      [:SELECT, 'SELECT'],
      [:NUMBER, 64],
    ])
  end

  it "lexes select str" do
    tokens =
    sql = "SELECT 'foo'"
    lex = Kwery::Parser::Lexer.new(sql)
    expect(lex.pairs).to eq([
      [:SELECT, 'SELECT'],
      [:STRING, 'foo'],
    ])
  end

  it "lexes select empty str" do
    tokens =
    sql = "SELECT ''"
    lex = Kwery::Parser::Lexer.new(sql)
    expect(lex.pairs).to eq([
      [:SELECT, 'SELECT'],
      [:STRING, ''],
    ])
  end

  it "lexes select escaped str" do
    tokens =
    sql = "SELECT '\\''"
    lex = Kwery::Parser::Lexer.new(sql)
    expect(lex.pairs).to eq([
      [:SELECT, 'SELECT'],
      [:STRING, "'"],
    ])
  end
end
