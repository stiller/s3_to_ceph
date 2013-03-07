#!/usr/bin/env/ruby
#Copyright (C) 2013 Joachim Nolten
#Permission is hereby granted, free of charge, to any person obtaining a copy of
#this software and associated documentation files (the "Software"), to deal in
#the Software without restriction, including without limitation the rights to
#use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
#of the Software, and to permit persons to whom the Software is furnished to do
#so, subject to the following conditions: The above copyright notice and this
#permission notice shall be included in all copies or substantial portions of
#the Software.  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
#EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
#MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO
#EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES
#OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
#ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
#DEALINGS IN THE SOFTWARE.

require 'logger'
require 'yaml'
require 'fog'
require 'pry'
require 'tempfile'
require 'parallel'

# No longer necesarry for next Fog release
module Excon
  class Connection
     VALID_CONNECTION_KEYS << :url
  end
end

Excon.defaults[:ssl_verify_peer] = false

OpenSSL::SSL::VERIFY_PEER = OpenSSL::SSL::VERIFY_NONE

module S3Backup

  @logger = Logger.new('backup.log', 'daily')
  STOR_CONF = {}

  confpath = ["#{ENV['S3CONF']}", Dir.home, "/etc/s3conf"]

  confpath.each do |path|
    if File.exists?(path) and File.directory?(path) and File.exists?("#{path}/s3config.yml")
      config = YAML.load_file("#{path}/s3config.yml")
      config.each_pair do |key, value|
        STOR_CONF[key] = value
      end
    end
  end

  s3 = Fog::Storage.new({
    :provider => 'AWS',
    :aws_access_key_id => STOR_CONF['AWS_ACCESS_KEY_ID'],
    :aws_secret_access_key => STOR_CONF['AWS_SECRET_ACCESS_KEY'],
    :region => 'eu-west-1',
    :path_style => true
  })

  ceph = Fog::Storage.new({
    :provider => 'AWS',
    :host       => 'ceph.webminded.nl',
    :aws_access_key_id     => STOR_CONF['CEPH_ACCESS_KEY_ID'],
    :aws_secret_access_key => STOR_CONF['CEPH_SECRET_ACCESS_KEY'],
    :path_style => true
  })

  @target_dir = ceph.directories.get('images.eu.viewbook.com')

  files = s3.directories.get('images.eu.viewbook.com').files


  subset = files.all
  subset.each_file_this_page
  @counter = 0

  def S3Backup.parallel_copy files
    Parallel.each(files, :in_threads => 128) do |s3_file|
      unless @target_dir.files.head(s3_file.key)
        @logger.info(s3_file.key)
        tempfile = Tempfile.new(s3_file.key)
        tempfile.write(s3_file.body)
        @target_dir.files.create(:key => s3_file.key, :body => tempfile )
        tempfile.unlink
      end
      @counter += 1
    end
  end

  parallel_copy subset
  while subset.is_truncated
    subset = subset.all(:marker => subset.last.key)
    parallel_copy subset
  end
  puts "Number of objects copied: #{@counter}"

end
