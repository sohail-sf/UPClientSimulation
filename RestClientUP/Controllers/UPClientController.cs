using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System.Collections.Generic;
using System.Linq;
using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Data.SqlClient;
using System.IO;
using System.Text;
using StackExchange.Redis;
using System.IO.Compression;
using Microsoft.IdentityModel.Protocols;
using Confluent.Kafka;
using System.Collections.Concurrent;
using System.Data;

namespace RestClientUP.Controllers
{
    [Route("api/[controller]/[action]")]
    [ApiController]
    public class UPClientController : ControllerBase
    {
        public IConfiguration Configuration { get; }
        private readonly CASRedisConnectionFactory _cASRedisConnectionFactory;
        public UPClientController(IConfiguration configuration, CASRedisConnectionFactory cASRedisConnectionFactory)
        {
            Configuration = configuration;
            _cASRedisConnectionFactory = cASRedisConnectionFactory;
        }

        [HttpPost]
        public async Task<IActionResult> Invoke()
        {
            try
            {
                await TestingSetup(Request);
                return Ok("Record inserted!");
            }catch(Exception ex)
            {
                return BadRequest(ex.Message);
            }
        }


        private async Task TestingSetup(HttpRequest Request)
        {
            var hotelId = Request.Query["HotelId"].ToString();
            var channelId = Request.Query["ChId"].ToString();
            var huid = Request.Query["Huid"].ToString();
            var groupId = Request.Query["GroupId"].ToString();
            var integrationId = Request.Query["IntegrationId"].ToString();
            var topicName = Request.Query["TopicName"].ToString();// optional
            var updatesCount = Request.Query["UpdateCount"].ToString();
            var updateMethod = Request.Query["UpdateMethod"].ToString();
            var updatedBy = Request.Query["UpdatedBy"].ToString();
            var clientId = Request.Query["ClientId"].ToString();

            var query = @$"INSERT INTO hotel_channel_update (HotelID, GroupID, IntegrationID, HotelUpdateID, ChannelID, UpdateCount, UpdateMethod, UpdatedBy)
                    VALUES
                    ('{hotelId}', '{groupId}', '{integrationId}', '{huid}', '{channelId}', '{updatesCount}', '{updateMethod}', '{updatedBy}');";

            // SQL logic
            await SQLInsertRecord(query);

            // Redis Logic
            var redisKey = String.Join('-',"CAQ", hotelId, channelId, huid);
            using (StreamReader reader = new StreamReader(Request.Body, Encoding.UTF8))
            {
                string requestBody = await reader.ReadToEndAsync();
                var compressData = await CompressString(requestBody);
                RedisInsertRecord(redisKey, compressData);
            }

            // Kafka logic
            string topicMessage = string.Join('#', huid, hotelId, channelId, clientId, integrationId);
            var topicDetailsList = await GetKafkaTopicDetails(Convert.ToInt32(hotelId));
            var topicDetail = topicDetailsList.Where(x => x.Channelid == Convert.ToInt32(channelId)).First();
            await SendMessageToKafka(topicMessage, topicDetail.TopicName, topicDetail.PartitionCount,hotelId);
        }

        private async Task SQLInsertRecord(string insertQuery)
        {
            var connStr = Configuration["ConnectionStrings:DefaultConnection"];
            using (SqlConnection connection = new SqlConnection(connStr))
            {
                using (SqlCommand command = new SqlCommand(insertQuery, connection))
                {
                    connection.Open();
                    command.ExecuteNonQuery();
                }
            }

            Console.WriteLine("Insert completed in SQL");
        }

        private void RedisInsertRecord(string key, string data)
        {

            TimeSpan expiry = new TimeSpan(TimeSpan.TicksPerDay);
            var redisDB = _cASRedisConnectionFactory.Connection.GetDatabase(12);
            redisDB.StringSet(key, data,expiry, flags:StackExchange.Redis.CommandFlags.PreferMaster);
            Console.WriteLine("Insert completed in Redis");

        }

        private async Task<string> CompressString(String s)
        {
            byte[] inputBytes = Encoding.UTF8.GetBytes(s);
            string outputbase64;
            using (var outputStream = new MemoryStream())
            {
                using (var gZipStream = new GZipStream(outputStream, CompressionMode.Compress))
                    await gZipStream.WriteAsync(inputBytes, 0, inputBytes.Length);
                byte[] outputBytes = outputStream.ToArray();
                outputbase64 = Convert.ToBase64String(outputBytes);
            }
            return outputbase64;
        }

        private async Task GetHotelSeqNo(long hotelid)
        {
            List<HotelSeqNo> liCSN = new List<HotelSeqNo>();
            var connStr = Configuration["ConnectionStrings:DefaultConnection"];
            using (SqlConnection connection = new SqlConnection(connStr))
            {
                using (SqlCommand command = new SqlCommand($"exec rezgain..GetClientSequenceLogic {hotelid}", connection))
                {
                    //command.CommandType = CommandType.StoredProcedure;
                    //command.Parameters.Add(new SqlParameter("@ParamName", hotelid));
                    connection.Open();

                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var hotelSeqNo = new HotelSeqNo()
                            {
                                Channelid = Convert.ToInt32(reader["ChannelId"]),
                                IsNewCSN = Convert.ToBoolean(reader["NewCSN"]),
                                HotelId = Convert.ToInt32(reader["HotelId"]),
                                ClientId = Convert.ToInt32(reader["ClientId"]),
                                IntegrationId = Convert.ToInt32(reader["IntegrationId"])
                            };
                            liCSN.Add(hotelSeqNo);
                        }
                    }
                }
            }

            Console.WriteLine("Insert completed.");
        }

        private async Task<List<TopicDetails>> GetKafkaTopicDetails(long hotelid)
        {
            List<TopicDetails> topicDetailsList = new List<TopicDetails>();
            var connStr = Configuration["ConnectionStrings:DefaultConnection"];
            using (SqlConnection connection = new SqlConnection(connStr))
            {
                using (SqlCommand command = new SqlCommand($"exec rezgain..USP_Get_KAFKATopicDetail_New {hotelid}", connection))
                {
                    //command.CommandType = CommandType.StoredProcedure;
                    //command.Parameters.Add(new SqlParameter("@ParamName", hotelid));
                    connection.Open();

                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var topic = new TopicDetails()
                            {
                                Channelid = Convert.ToInt32(reader["ChannelID"]),
                                TopicName = reader["TopicName"].ToString(),
                                PartitionCount = Convert.ToInt32(reader["PartitionCount"]),
                            };
                            topicDetailsList.Add(topic);
                        }
                    }
                }
            }

           return topicDetailsList;
        }

        private async Task SendMessageToKafka(string message,string topicname, int totalPartition, string hotelid)
        {
            var config = new ProducerConfig
            {
                //   GroupId = "" + ConfigurationManager.AppSettings["GroupId"],// "ARIFILES",
                BootstrapServers = "" + Configuration["Kafka:Bootstrapserver"],
                QueueBufferingMaxMessages = 300,
                BatchNumMessages = 1,
                CompressionLevel = 2,
                Acks = Acks.All,
                MessageTimeoutMs = 10000,
                LingerMs = 0
            };
            topicname = string.IsNullOrEmpty(topicname)? Configuration["Kafka:DefaultTopic"] :topicname;
            var partitionId = totalPartition == 0 ? totalPartition: Convert.ToInt32(hotelid) % totalPartition;
            using (var p = new ProducerBuilder<string, string>(config).Build())
            {
                Message<string, string> msg = new Message<string, string>();
                msg.Key = hotelid; msg.Value = message;
                Partition objPartition = new Partition(partitionId);
                TopicPartition objTopicPartition = new TopicPartition(topicname, objPartition);
                 await p.ProduceAsync(objTopicPartition, msg);
                 p.Flush();
            }

            Console.WriteLine("Insert completed in Kafka");

        }
    }

    public class HotelSeqNo
    {
        public int Channelid { get; set; }
        public bool IsNewCSN { get; set; }
        public int HotelId { get; set; }
        public int ClientId { get; set; }
        public int IntegrationId { get; set; }

    }

    public class TopicDetails
    {
        public int Channelid { get; set; }
        public string TopicName { get; set; }
        public int PartitionCount { get; set; }
    }
}
