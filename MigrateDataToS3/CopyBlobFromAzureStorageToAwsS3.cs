using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.Runtime;
using System.IO;
using System.Net.Http;

namespace MigrateDataToS3;

public class CopyBlobFromAzureStorageToAwsS3
{
    [FunctionName(nameof(CopyBlobFromAzureStorageToAwsS3))]
    public static async Task<IActionResult> Run(
        [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] CopyBlobRequest req,
        ILogger log)
    {
        log.LogInformation("C# HTTP trigger function processed a request.");

        var credentials = new BasicAWSCredentials(req.AwsAccessKey, req.AwsSecretKey);
        var config = new AmazonS3Config
        {
            RegionEndpoint = Amazon.RegionEndpoint.GetBySystemName(req.AwsRegion)
        };
        var client = new AmazonS3Client(credentials, config);
        await UploadFileAsync(client, req.BucketName, req.ObjectName, req.AzSasUrl);

        return new OkResult();
    }

    private static async Task UploadFileAsync(
        IAmazonS3 client,
        string bucketName,
        string objectName,
        string filePath)
    {
        string temporaryFileUrl = string.Empty;
        try
        {
            temporaryFileUrl = await DownloadFile(filePath);
            var request = new PutObjectRequest
            {
                BucketName = bucketName,
                Key = objectName,
                FilePath = temporaryFileUrl,
                CannedACL = S3CannedACL.PublicRead
            };

            var response = await client.PutObjectAsync(request);
            if (response.HttpStatusCode == System.Net.HttpStatusCode.OK)
            {
                Console.WriteLine($"Successfully uploaded {objectName} to {bucketName}.");
            }
            else
            {
                Console.WriteLine($"Could not upload {objectName} to {bucketName}.");
            }
        }
        finally
        {
            File.Delete(temporaryFileUrl);
        }
    }

    private static async Task<string> DownloadFile(string url)
    {
        using var client = new HttpClient();
        using var response = await client.GetAsync(url);
        response.EnsureSuccessStatusCode();
        using var stream = await response.Content.ReadAsStreamAsync();
        string fileName = string.Format("{0}{1}", Guid.NewGuid(), Path.GetExtension(RemoveQueryParams(url)));
        string tempFilePath = Path.Combine(Path.GetTempPath(), fileName);
        using var fileStream = new FileStream(tempFilePath, FileMode.Create, FileAccess.Write);
        await stream.CopyToAsync(fileStream);
        return tempFilePath;
    }

    private static string RemoveQueryParams(string url) => new Uri(url).GetLeftPart(UriPartial.Path);
}

public record CopyBlobRequest
{
    public string AwsRegion { get; init; } = "ap-southeast-2";
    public string AwsAccessKey { get; init; }
    public string AwsSecretKey { get; init; }
    public string BucketName { get; init; }
    public string ObjectName { get; set; }
    public string AzSasUrl { get; set; }
}


