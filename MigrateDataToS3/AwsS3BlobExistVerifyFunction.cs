using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon;
using Amazon.Runtime;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using System.Collections.Generic;
using System.Linq;

namespace UtilityFunctions;

public class AwsS3BlobExistVerifyFunction
{
    private const string bucketName = "";
    private const string accessKey = "";
    private const string secretKey = "";

    private readonly IConfiguration _configuration;
    public AwsS3BlobExistVerifyFunction(IConfiguration configuration)
    {
        _configuration = configuration;
    }
    [FunctionName(nameof(AwsS3BlobExistVerifyFunction))]
    public async Task<IActionResult> RunAsync(
        [HttpTrigger(AuthorizationLevel.Function, "get", Route = null)] HttpRequest req,
        [Sql(commandText: "SELECT [Id],[DatabaseName],[HasAttachment] FROM [dbo].[MigratedInspection] WHERE HasAttachment=1",
            commandType: System.Data.CommandType.Text,
            connectionStringSetting: "SqlConnectionString")] IEnumerable<MigratedInspection> migratedInspections,
        [Queue("batch-process")] IAsyncCollector<string> batchProcessRequests,
        ILogger log)
    {
        log.LogInformation("C# HTTP trigger function processed a request.");

        var tasks = migratedInspections.Select(inspection => batchProcessRequests.AddAsync(inspection.Id.ToString()));
        await Task.WhenAll(tasks);

        return new OkObjectResult($"{migratedInspections.Count()} blobs sent for verification");
    }

    [FunctionName(nameof(AwsS3BlobExistVerifyFunction) + "test")]
    public async Task TestAsync(
        [QueueTrigger("batch-process")] string queueMessage,
        [Sql(commandText: "SELECT * FROM dbo.MigratedInspection WHERE HasAttachment=1 AND Id=@Id",
            parameters: "@Id={queueTrigger}",
            connectionStringSetting: "SqlConnectionString")]
        IEnumerable<MigratedInspection> migratedInspections,
        ILogger log)
    {
        log.LogInformation("C# Queue trigger function processed for Inspection: {0}", queueMessage);

        var inspection = migratedInspections.First();
        var previousState = inspection.HasAttachment;

        await VerifyWhetherBlobExistAsync(inspection);

        if (previousState.Equals(inspection.HasAttachment))
        {
            log.LogInformation("Inspection: {0} -> Skipped", queueMessage);
            return;
        }

        await ChangeHasAttachmentFlagAsync(inspection);
    }

    private async Task ChangeHasAttachmentFlagAsync(MigratedInspection inspection)
    {
        using var connection = new SqlConnection(_configuration.GetValue<string>("SqlConnectionString"));
        using var command = connection.CreateCommand();
        command.CommandText = "UPDATE dbo.MigratedInspection SET HasAttachment=@hasAttachment WHERE Id=@id";
        command.Parameters.AddWithValue("@hasAttachment", inspection.HasAttachment);
        command.Parameters.AddWithValue("@id", inspection.Id);

        connection.Open();
        await command.ExecuteNonQueryAsync();
        connection.Close();
    }

    private async Task VerifyWhetherBlobExistAsync(MigratedInspection inspection)
    {
        inspection.HasAttachment = await CheckBlobExistsAsync(bucketName, inspection.FilePath);
    }

    private async Task<bool> CheckBlobExistsAsync(string bucketName, string blobKey)
    {
        using var client = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey), new AmazonS3Config
        {
            RegionEndpoint = RegionEndpoint.APSoutheast2
        });
        var request = new GetObjectMetadataRequest
        {
            BucketName = bucketName,
            Key = blobKey
        };
        try
        {
            await client.GetObjectMetadataAsync(request);
            return true;
        }
        catch (AmazonS3Exception ex)
        {
            if (ex.StatusCode == System.Net.HttpStatusCode.NotFound) return false;

            throw; // Some other exception occurred
        }
    }
}

public record MigratedInspection
{
    public int Id { get; set; }
    public string DatabaseName { private get; set; }
    public bool HasAttachment { get; set; }
    internal string FilePath => $"{DatabaseName}/{Id}.pdf";
}
