using HtmlAgilityPack;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.Xml;

namespace fd.Coins.TagsExtract
{
    class Program
    {
        static void Main(string[] args)
        {
            var offset = 0;
            var containsData = true;
            var sb = new StringBuilder();
            while (containsData)
            {
                // Address of URL
                string URL = $"https://www.blockchain.com/btc/tags?filter=8&offset={offset}";
                offset += 50;
                var web = new HtmlWeb();
                var doc = web.Load(URL);
                var html = doc.DocumentNode.ChildNodes.First(x => x.Name == "html");
                var body = html.ChildNodes.First(x => x.Name == "body");
                var content = body.ChildNodes.First(x => x.Name == "div");
                var table = content.ChildNodes.First(x => x.Name == "table");
                var tbody = table.ChildNodes.First(x => x.Name == "tbody");
                containsData = tbody.ChildNodes.OfType<HtmlNode>().Count() > 0;
                foreach(var line in tbody.ChildNodes.Where(x => x.GetType() != typeof(HtmlTextNode)))
                {
                    var items = line.ChildNodes.Select(x => x.ChildNodes.Select(y => y.Name).Contains("img") ? x.LastChild.Attributes["src"].Value.Contains("red") ? false.ToString() : true.ToString() : x.InnerText.Trim()).Where(x => !string.IsNullOrEmpty(x));
                    var row = string.Join("\t", items);
                    if (bool.Parse(items.Last()))
                    {
                        Console.WriteLine(row);
                        sb.AppendLine(row);
                    }
                }
            }
            File.WriteAllText("tags.txt", sb.ToString());
            Console.WriteLine("DONE");
            Console.Read();
        }
    }
}
