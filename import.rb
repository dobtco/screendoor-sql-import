require 'dotenv'
require 'httparty'
require 'json'
require 'tiny_tds'

Dotenv.load

HTTParty::Basement.default_options.update(verify: false)

# Modify this stuff for your use-case:

# client = TinyTds::Client.new(
#   username: ENV['SQL_USER'],
#   password: ENV['SQL_PW'],
#   host: ENV['SQL_HOST'],
#   database: ENV['SQL_DB'],
# )

TABLE_NAME = 'dbo.IFS_SLRData'
API_BASE = 'https://screendoor.dobt.co'
API_KEY = ENV['SCREENDOOR_API_KEY']
PER_PAGE = 100
PROJECT_ID = 1712
OPEN_STATUS_NAME = 'Open'
CLOSED_STATUS_NAME = 'Closed'

def response_text(record, field_id)
  record['responses'][field_id.to_s]
end

def response_hash(record, field_id)
  hash = record['responses'][field_id.to_s]

  if hash.is_a?(Hash)
    hash
  else
    {}
  end
end

def transform_phone(text)
  if text
    text.scan(/\d/).join
  end
end

def transform_record(record)
  {
    'LASTUSER' => 'import',
    'CONSUMERFNAME' => response_text(record, 19580),
    'CONSUMERLNAME' => response_text(record, 19582),
    'CONSUMERADDRESS' => response_hash(record, 19583)['street'],
    'CONSUMERCITY' => response_hash(record, 19583)['city'],
    'CONSUMERSTATE' => response_hash(record, 19583)['state'],
    'CONSUMERZIP' => response_hash(record, 19583)['zipcode'],
    'CONSUMERPHONE' => transform_phone(response_text(record, 19588)),
    'CONSUMEREMAIL' => response_text(record, 19584),
    'BUSINESSNAME' => response_text(record, 19586),
    'BUSINESSADDRESS' => response_hash(record, 19587)['street'],
    'BUSINESSCITY' => response_hash(record, 19587)['city'],
    'BUSINESSSTATE' => response_hash(record, 19587)['state'],
    'BUSINESSZIP' => response_hash(record, 19587)['zipcode'],
    'BUSINESSPHONE' => transform_phone(response_text(record, 19593)),
    'NAMEOFSCHOOLS' => response_text(record, 19592),
    'COMPLAINTINFO' => response_text(record, 19627),
    'TYPEOFLOAN' => response_text(record, 19594),
    'FEDLOANTYPE' => response_text(record, 19605),
    'SLACCOUNTNUMBER' => response_text(record, 19606),
    'FEDLOANDEFAULTYN' => response_text(record, 19614),
    'WAGEGARNISHEDYN' => response_text(record, 19617),
    'FEDLOANDEFERNMENT' => response_text(record, 19621),
    'SEEKINGASSISTANCEYN' => response_text(record, 19626)
  }
end

# You shouldn't have to modify anything below this line:

def make_request(records, page)
  puts "Getting page #{page}..."

  resp = HTTParty.get(
    "#{API_BASE}/api/projects/#{PROJECT_ID}/responses.json",
    query: {
      v: 0,
      api_key: API_KEY,
      page: page,
      per_page: PER_PAGE,
      status: OPEN_STATUS_NAME
    }
  )

  parsed_body = JSON::parse(resp.body)

  if parsed_body.is_a?(Array)
    records.push(*parsed_body)
  else
    fail "Error getting records: #{resp.inspect}"
  end

  resp
rescue JSON::ParserError
  fail "Error getting records: #{resp.inspect}"
end

def mark_as_closed(id)
  resp = HTTParty.put(
    "#{API_BASE}/api/projects/#{PROJECT_ID}/responses/#{id}.json",
    query: {
      v: 0,
      api_key: API_KEY,
      status: CLOSED_STATUS_NAME
    }
  )

  unless resp.code == 200
    fail "Failed to mark response #{id} as closed."
  end
end

records = []

resp = make_request(records, 1)
total_pages = (resp.headers['Total'].to_f / PER_PAGE).ceil

if total_pages > 1
  (2..total_pages).each do |page|
    make_request(records, page)
  end
end

if records.length > 0
  puts "Found #{records.length} new records."
else
  puts "No new records!"
  exit
end

records.each do |record|
  response_hash = transform_record(record)

  puts response_hash

  # client.execute %{
  #   INSERT INTO #{TABLE_NAME} (LASTMODIFIED,#{response_hash.keys.join(',')})
  #   VALUES(GETDATE(),#{response_hash.values.map { |v| "'" + client.escape(v) + "'" }.join(',')})
  # }

  mark_as_closed(record['id'])
end

puts "Updated status to closed. All done!"
