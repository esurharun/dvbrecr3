require 'projectx.jar'
require 'logback-classic-0.9.8.jar'
require 'logback-core-0.9.8.jar'
require 'mina-core-1.1.6.jar'
require 'red5.jar'
require 'slf4j-api-1.4.3.jar'
require 'xercesImpl-2.9.0.jar'
require 'java'
require 'benchmark'
require 'FileUtils'

SEPARATOR_ = '\\'

module JavaConcurrent
  include_package 'java.util.concurrent'
end

module JavaUTIL
  include_package 'java.util'
end

$executor = JavaConcurrent::Executors.new_fixed_thread_pool(6)

class String
  def to_bool
    return true if self == true || self =~ (/(true|t|yes|y|1)$/i)
    return false if self == false ||  self =~ (/(false|f|no|n|0)$/i)
    raise ArgumentError.new("invalid value for Boolean: \"#{self}\"")
  end
end


def shutdown
  puts 'Shutting down..'
  $executor.shutdown
  exit(0)
end

MENCODER_MPG_CMD = 'mencoder -forceidx -tskeepbroken -oac lavc -ovc lavc -of mpeg -mpegopts format=xvcd -vf fixpts,scale=352:288 -srate 44100 -af lavcresample=44100 -lavcopts vcodec=mpeg1video:keyint=15:vrc_buf_size=327:vrc_minrate=1152:vbitrate=1152:vrc_maxrate=1152:acodec=mp2:abitrate=224:aspect=16/9:threads=4:turbo -ofps 25 -o %s %s 2>&1'
MENCODER_FLV_CMD = 'mencoder -forceidx -of lavf -oac mp3lame -lameopts abr:br=56 -srate 22050 -ovc lavc -lavcopts vcodec=flv:vbitrate=250:mbd=2:mv0:trell:v4mv:cbp:last_pred=3 -o %s %s 2>&1'

Signal.trap 'SIGTERM' do
  shutdown
end

TS_FILE_HEADER = 'tsdump'

def fail_and_shutdown(message)
  puts "#{@properties_filename} :  #{message}"
  shutdown
end

class Dvbr3



  def check_config_and_process
    if @config.size == 0
      fail_and_shutdown 'Configuration is empty'
    else
      @tuner_id = "#{@config['TUNERID']}".to_i
      @channel_name = @config['CHANNELNAME']
      @audio_pid = @config['AID']
      @video_pid = @config['VID']
      @mpg_path = @config['RECPATH']
      @flv_enabled = @config['FLV_ENABLED'].to_bool
      @flv_path = @config['FLV_RECPATH']
      @lookup_path = @config['LOOKUP_PATH']

      missing_txt = 'Missing %s in configuration file'

      fail_and_shutdown(missing_txt % %w('TUNERID')) unless @tuner_id
      fail_and_shutdown(missing_txt % %w('CHANNELNAME')) unless @channel_name
      fail_and_shutdown(missing_txt % %w('AID')) unless @audio_pid
      fail_and_shutdown(missing_txt % %w('VID')) unless @video_pid
      fail_and_shutdown(missing_txt % %w('RECPATH')) unless @mpg_path
      fail_and_shutdown(missing_txt % %w('FLV_ENABLED')) if @flv_enabled == nil
      fail_and_shutdown(missing_txt % %w('FLV_RECPATH')) unless @flv_path
      fail_and_shutdown(missing_txt % %w('LOOKUP_PATH')) unless @lookup_path

      not_exists_text = '%s is not exists'

      fail_and_shutdown(not_exists_text % [@mpg_path]) unless File.exist?(@mpg_path)
      fail_and_shutdown(not_exists_text % [@flv_path]) unless File.exist?(@flv_path)
      fail_and_shutdown(not_exists_text % [@lookup_path]) unless File.exist?(@lookup_path)


      not_writable_text = '%s is not writable'

      fail_and_shutdown(not_writable_text % [@mpg_path]) unless File.writable?(@mpg_path)
      fail_and_shutdown(not_writable_text % [@flv_path]) unless File.writable?(@flv_path)
    end

  end

  def create_metadata_for_flv(in_file)

    puts "#{@channel_name} (FLV_MD) : Generating metadata for #{in_file}"

    Benchmark.bm do |x|

      x.report('FLV_MD:') {

        file = Java::JavaIo::File.new(in_file)

        flvReader = Java::OrgRed5IoFlvImpl::FLVReader.new(file)

        metaCache = Java::OrgRed5Io::FileKeyFrameMetaCache.new

        metaCache.saveKeyFrameMeta(file, flvReader.analyzeKeyFrames())
        flvReader.close()
      }
    end


    puts "#{@channel_name} (FLV_MD) : Finished generating metadata for #{in_file}"
  end

  def create_flv(in_file,out_file)

    puts "#{@channel_name} (FLV) : Started encoding #{in_file} to #{out_file}"

    command = MENCODER_FLV_CMD % [out_file, in_file]

    Benchmark.bm do |x|

      x.report('FLV:') {

        output = `#{command}`

        File.open("#{out_file}.encflv.log.txt", "w") do |f|
          f.write output
       end
      }
    end

    puts "#{@channel_name} (FLV) : Finished encoding #{in_file} to #{out_file}"

  end


  def create_mpg(in_file,out_file)

    puts "#{@channel_name} (MPG) : Encoding #{in_file} to #{out_file}"

    command = MENCODER_MPG_CMD % [out_file, in_file]

    Benchmark.bm do |x|

      x.report('MPG:') {

	  #puts command
        output = `#{command}`

        File.open("#{out_file}.encmpg.log.txt", "w") do |f|
          f.write output
        end
      }
    end


    puts "#{@channel_name} (MPG) : Finished encoding #{in_file} to #{out_file}"

  end

  def pre_process(in_file)

    puts "#{@channel_name} : Preprocessing #{in_file}"

    Benchmark.bm do |x|

      x.report('PRE: ') {

        `java -jar projectx.jar -tots -id #{@audio_pid},#{@video_pid} #{in_file}`
      }

    end

  end

  def run
    $executor.execute do
      while true

        Dir.foreach(@lookup_path) do |item|
          next if item == '.' or item == '..'

          full_path = "#{@lookup_path}#{SEPARATOR_}#{item}"

          match = item.match(/#{TS_FILE_HEADER}_(\d+)_(\d+)-(\d+).ts$/i)

          if match
            c_tuner_id = "#{match.captures[0]}".to_i
            c_start_time = "#{match.captures[1]}".to_i
            c_stop_time = "#{match.captures[2]}".to_i

            if c_tuner_id == @tuner_id

              calendar_st = JavaUTIL::Calendar.getInstance()
              calendar_st.setTimeInMillis(c_start_time)

              calendar_en = JavaUTIL::Calendar.getInstance()
              calendar_en.setTimeInMillis(c_stop_time)

              st_year = calendar_st.get(JavaUTIL::Calendar::YEAR)
              st_month = calendar_st.get(JavaUTIL::Calendar::MONTH)+1
              st_day = calendar_st.get(JavaUTIL::Calendar::DAY_OF_MONTH)

              out_fn_body = '-%s%02d%02d_%02d%02d%02d-%02d%02d%02d' %
                  [st_year,
                   st_month,
                  st_day,
                  calendar_st.get(JavaUTIL::Calendar::HOUR_OF_DAY),
                  calendar_st.get(JavaUTIL::Calendar::MINUTE),
                  calendar_st.get(JavaUTIL::Calendar::SECOND),
                  calendar_en.get(JavaUTIL::Calendar::HOUR_OF_DAY),
                  calendar_en.get(JavaUTIL::Calendar::MINUTE),
                  calendar_en.get(JavaUTIL::Calendar::SECOND)
									]

              out_mpg_fn = "CH%s#{out_fn_body}.mpg" % [c_tuner_id]
              out_flv_fn = "%s#{out_fn_body}.flv" % [@channel_name]

              pre_process(full_path)

              mpg_out_path =   "#{@lookup_path}#{SEPARATOR_}#{out_mpg_fn}"
              flv_out_path =   "#{@lookup_path}#{SEPARATOR_}#{out_flv_fn}"

              create_mpg("#{full_path}_remux.ts",mpg_out_path)

              if @flv_enabled

                  if File.exist?(mpg_out_path)
                    create_flv(mpg_out_path, flv_out_path)



                  else
                    puts "#{@channel_name} : FATAL ERROR #{mpg_out_path} could not found!!!"
                  end


              end

              if  File.exist? mpg_out_path

			
		    if File.exist? flv_out_path and @flv_enabled
                	create_metadata_for_flv(flv_out_path)

                	flv_dest_path = "%s#{SEPARATOR_}%s#{SEPARATOR_}%02d#{SEPARATOR_}%02d#{SEPARATOR_}" % [@flv_path,st_year,st_month,st_day]

                	FileUtils.mkdir_p flv_dest_path
		    end
                puts "#{@channel_name} : Moving #{flv_out_path}" if @flv_enabled
                Benchmark.bm do |x|

                  x.report('MOVE_FLV: ') {
                    FileUtils.mv(flv_out_path, "#{flv_dest_path}#{SEPARATOR_}#{out_flv_fn}") if @flv_enabled
                    FileUtils.cp(flv_out_path+'.meta', "#{flv_dest_path}#{SEPARATOR_}#{out_flv_fn}.meta") if @flv_enabled



                  }
                end

                puts "#{@channel_name} : Moving #{mpg_out_path}"
                Benchmark.bm do |x|

                  x.report('MOVE_MPG: ') {
                    FileUtils.mv(mpg_out_path, "#{@mpg_path}#{SEPARATOR_}#{out_mpg_fn}")
                  }
                end



                puts "#{@channel_name} : Removing #{full_path}"
                File.delete full_path

                puts "#{@channel_name} : Removing #{full_path}_remux.ts"
                File.delete "#{full_path}_remux.ts"




              end


            end



          end


        end


        sleep 1.0
      end
    end
  end

  def initialize(properties_filename)
    @properties_filename = properties_filename
    @config = load_properties(@properties_filename)

    check_config_and_process

    run

  end

  def load_properties(properties_filename)
    properties = {}
    File.open(properties_filename, 'r') do |properties_file|
      properties_file.read.each_line do |line|
        line.strip!
        if line[0] != ?# and line[0] != ?=
          i = line.index('=')
          if i
            properties[line[0..i - 1].strip] = line[i + 1..-1].strip
          else
            properties[line] = ''
          end
        end
      end
    end
    properties
  end

end

fail_and_shutdown 'Please enter a configuration file name as parameter' unless ARGV[0]

fail_and_shutdown '%s does not exists' % [ARGV[0]] unless File.exist? ARGV[0]


puts "Started on #{ARGV[0]}"

dvbr = Dvbr3.new(ARGV[0])




#start = Java::NetSourceforgeDvbProjectxCommon::Start

#start.main(['-tots','-id','166,165', '/Users/marcus/tmp/tsdump_0_1362837602097-1362844802019.ts'])





