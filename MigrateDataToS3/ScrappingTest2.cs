using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using System.Net.Http;
using System.Net;
using HtmlAgilityPack;
using System.Collections.Generic;
using System.Linq;

namespace UtilityFunctions;

public class ScrappingTest2
{
    private bool endofthepage = false;
    [FunctionName("ScrappingTest2")]
    public async Task<IActionResult> Run(
        [HttpTrigger(AuthorizationLevel.Function, "get", Route = null)] HttpRequest req,
        ILogger log)
    {
        log.LogInformation("C# HTTP trigger function processed a request.");


        var handler = new HttpClientHandler()
        {
            CookieContainer = new CookieContainer()
        };
        var baseUrl = new Uri("https://test.inspectionmanager.com");
        handler.CookieContainer.Add(baseUrl, new Cookie("ASP.NET_SessionId", "d5eryr1kwqhmjdhzdf3dk5s2"));
        handler.CookieContainer.Add(baseUrl, new Cookie("__RequestVerificationToken", "kKypqi6D_DI0TSxQjp4AnsrMAusnxVkfZpm3PAgRELWejlvyXpuvhTN2kWqI7L0PcVJh2MuU-X5WY7AfBc9c5qrY0iM1"));
        handler.CookieContainer.Add(baseUrl, new Cookie(".ASPXAUTH", "4775610A937A308F85E04A34E09B99472E9B0EF58B455D410BB124B6DA0938EF88A270681603CF7109D6B1C349D18C3C51BE17C371BDC7FBF6B9B3C57FD8B56C39C93CD8CA7CE4CE207DCD8D7231DE210B861688B8771212DB1E1DFD47E7F7AE7502FCBD"));

        HttpClient client = new(handler);

        var inspectionRecords = new List<InspectionRecord>();

        int pageNo = 1;
        string url = "https://test.inspectionmanager.com/Inspection/FilterInspections?pn={0}&srtfld=&srtord=&toIncludeInspectionIds=&mgr=&suburb=&pMgr=&ps=20";
        while (!endofthepage)
        {
            await ExtractPage(client, String.Format(url, pageNo++), inspectionRecords);
        }


        return new OkObjectResult(inspectionRecords);
    }

    private async Task ExtractPage(HttpClient client, string url, List<InspectionRecord> inspectionRecords)
    {
        HttpResponseMessage response = await client.GetAsync(url);
        if (!response.IsSuccessStatusCode)
        {
            endofthepage = true;
            Console.WriteLine("Failed to scrape the content");
            return;
        }


        string content = await response.Content.ReadAsStringAsync();
        HtmlDocument doc = new();
        doc.LoadHtml(content);


        HtmlNode table = doc.DocumentNode.SelectSingleNode("//tbody");
        HtmlNodeCollection rows = table.SelectNodes(".//tr");
        

        foreach (var row in rows)
        {
            var tds = row.SelectNodes(".//td");
            if (tds is null)
            {
                if (rows.Count == 1) endofthepage = true;
                continue;
            }

            inspectionRecords.Add(ExtractDataFromRow(tds));
        }
    }

    private static InspectionRecord ExtractDataFromRow(HtmlNodeCollection nodes)
    {
        var inspection = new InspectionRecord();
        foreach (var (td, index) in nodes.WithIndex())
        {
            if (td is null) continue;
            if (index == 0)
            {
                var element = td.Descendants().Where(node => node.HasClass("Inspection_InspectionID")).FirstOrDefault();
                inspection.Id = element?.GetAttributeValue<string>("value", "");
            }
            if (index == 1)
            {
                HtmlNode label = td.SelectSingleNode(".//label");
                inspection.PropertyId = label.InnerHtml;
            }
            else if (index == 2) inspection.Address = td.InnerHtml;
            else if (index == 3) inspection.Suburb = td.InnerHtml;
            else if (index == 4) inspection.Inspector = td.InnerHtml;
            else if (index == 6)
            {
                HtmlNode icon = td.SelectSingleNode(".//label").SelectNodes(".//i")[1];
                inspection.Status = icon.InnerHtml;
            }
            else if (index == 7)
            {
                HtmlNode div = td.SelectSingleNode(".//div");
                td.RemoveChild(div);
                inspection.Type = td.InnerText.Trim();
            }
            else if (index == 8) inspection.Date = td.InnerHtml.Trim();
            else if (index == 9) inspection.Time = td.InnerHtml.Trim();
        }
        return inspection;
    }
}

public static class EnumExtension
{
    public static IEnumerable<(T item, int index)> WithIndex<T>(this IEnumerable<T> self) => self.Select((item, index) => (item, index));
}


public record InspectionRecord
{
    public string Id { get; set; }
    public string PropertyId { get; set; }
    public string Address { get; set; }
    public string Suburb { get; set; }
    public string Inspector { get; set; }
    public string Status { get; set; }
    public string Type { get; set; }
    public string Date { get; set; }
    public string Time { get; set; }
}