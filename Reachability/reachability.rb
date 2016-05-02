# ruby version

require 'set'

print 'Name: '
name = gets.chomp.upcase

print '#Hops: '
hops = gets.chomp.to_i

lines = File.open('data/friend1.txt', &:read).split("\n")

pairs = lines.flat_map do |line|
  names = line.split(',')
  [names, names.reverse]
end

table = {}

pairs.each do |p|
  table[p.first] = table[p.first].nil? ? [p.last] : (table[p.first] + [p.last])
end

results = Set.new([name])

hops.times do
  results = results.union(results.map { |n| table[n] }.flatten)
end

p results.to_a.sort
