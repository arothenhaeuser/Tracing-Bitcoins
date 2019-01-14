using HtmlAgilityPack;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;

namespace fd.Coins.TagsExtract
{
    class Program
    {
        static void Main(string[] args)
        {
            TxHashExtract2(@"C:\Users\Rothenhaeuser\source\repos\fd.Coins\fd.Coins.TagsExtract\bin\Debug\error.txt");
            //Parse();
            //Clean();
        }

        static void Clean()
        {
            var lines = File.ReadAllLines("tags.txt");
            var table = lines.Select(x => x.Split('\t'));
            var lookup = table.ToLookup(x => x[2], x => x[0]);
            var sb = new StringBuilder();
            foreach(var entry in lookup)
            {
                if(entry.Count() > 1)
                {
                    sb.AppendLine(string.Join("\t", entry.ToList()));
                }
            }
            File.WriteAllText("gold (tags).txt", sb.ToString());
            Console.Read();
        }
        static void TxHashExtract2(string pathToHashes)
        {
            var hashes = File.ReadAllLines(pathToHashes).Where(x => !string.IsNullOrEmpty(x)).Distinct().ToList();
            var nextHashes = new List<string>();
            var web = new HtmlWeb();
            foreach (var hash in hashes)
            {
                Console.WriteLine(hash);
                try
                {
                    var URL = $"https://www.blockchain.com/btc/tx/{hash}?show_adv=true";
                    var doc = web.Load(URL);
                    var nextURLs = doc.DocumentNode.Descendants().Where(x => x.Name == "a").Where(x => x.InnerText == "Spent").Select(x => "https://www.blockchain.com" + x.Attributes["href"].Value);
                    var distinctURLs = nextURLs.Distinct().ToList();
                    foreach (var next in distinctURLs)
                    {
                        var nextDoc = web.Load(next);
                        var nextHash = nextDoc.DocumentNode.Descendants().First(x => x.Name == "title").InnerText.Split(' ').Last();
                        nextHash.Trim();
                        File.AppendAllText("nexthashes.txt", nextHash + "\n");
                    }
                }
                catch(Exception e)
                {
                    try
                    {
                        var URL = $"https://www.blockchain.com/btc/tx/{hash}?show_adv=true";
                        var doc = web.Load(URL);
                        var nextURLs = doc.DocumentNode.Descendants().Where(x => x.Name == "a").Where(x => x.InnerText == "Spent").Select(x => "https://www.blockchain.com" + x.Attributes["href"].Value);
                        foreach (var next in nextURLs.Distinct())
                        {
                            var nextDoc = web.Load(next);
                            var nextHash = nextDoc.DocumentNode.Descendants().First(x => x.Name == "title").InnerText.Split(' ').Last();
                            nextHash.Trim();
                            File.AppendAllText("nexthashes.txt", nextHash + "\n");
                        }
                    }
                    catch
                    {
                        Console.WriteLine($"Error on {hash}");
                        File.AppendAllText("error.txt", hash + "\n");
                    }
                }
            }
        }
        static void TxHashExtract(string pathToAddresses)
        {
            var addresses = File.ReadAllLines(pathToAddresses).SelectMany(x => x.Split('\t')).Where(x => !string.IsNullOrEmpty(x));
            var sb = new StringBuilder();
            foreach (var address in addresses)
            {
                try
                {
                    var URL = $"https://www.blockchain.com/btc/address/{address}";
                    var web = new HtmlWeb();
                    var doc = web.Load(URL);
                    var txContainer = doc.GetElementbyId("tx_container");
                    List<string> hashes = txContainer.Descendants().Where(x => x.HasClass("hash-link")).Select(x => x.InnerText).ToList();
                    if(txContainer.Descendants().Any(x => x.HasClass("pagination")))
                    {
                        var urlPart = txContainer.Descendants().Where(x => x.HasClass("pagination")).First().ChildNodes.Where(x => x.GetClasses().Count() == 0).Where(x => x.Name == "li").Select(x => x.FirstChild.Attributes["href"].Value);
                        foreach (var part in urlPart)
                        {
                            var newURL = URL + part;
                            var newWeb = new HtmlWeb();
                            var newDoc = web.Load(URL);
                            var newTxContainer = doc.GetElementbyId("tx_container");
                            hashes.AddRange(txContainer.Descendants().Where(x => x.HasClass("hash-link")).Select(x => x.InnerText));
                        }
                    }
                    foreach(var hash in hashes)
                    {
                        sb.AppendLine(hash);
                    }
                }
                catch
                {
                    try
                    {
                        Thread.Sleep(25);
                        var URL = $"https://www.blockchain.com/btc/address/{address}";
                        var web = new HtmlWeb();
                        var doc = web.Load(URL);
                        var txContainer = doc.GetElementbyId("tx_container");
                        List<string> hashes = txContainer.Descendants().Where(x => x.HasClass("hash-link")).Select(x => x.InnerText).ToList();
                        if (txContainer.Descendants().Any(x => x.HasClass("pagination")))
                        {
                            var urlPart = txContainer.Descendants().Where(x => x.HasClass("pagination")).First().ChildNodes.Where(x => x.GetClasses().Count() == 0).Select(x => x.FirstChild.Attributes["href"].Value);
                            foreach (var part in urlPart)
                            {
                                var newURL = URL + part;
                                var newWeb = new HtmlWeb();
                                var newDoc = web.Load(URL);
                                var newTxContainer = doc.GetElementbyId("tx_container");
                                hashes.AddRange(txContainer.Descendants().Where(x => x.HasClass("hash-link")).Select(x => x.InnerText));
                            }
                        }
                        foreach (var hash in hashes)
                        {
                            sb.AppendLine(hash);
                        }
                    }
                    catch
                    {
                        Console.WriteLine($"Error on {address}");
                        File.AppendAllText("error.txt", address + "\n");
                    }
                }
            }
            File.WriteAllText("hashes.txt", sb.ToString());
        }
        static void TimeLineExtract(string pathToAddresses)
        {
            var addresses = File.ReadAllLines(pathToAddresses).SelectMany(x => x.Split('\t')).Where(x => !string.IsNullOrEmpty(x));
            var sb = new StringBuilder();
            foreach(var address in addresses)
            {
                try
                {
                    var URLFirst = $"https://www.blockchain.com/btc/address/{address}?sort=1";
                    var webFirst = new HtmlWeb();
                    var docFirst = webFirst.Load(URLFirst);
                    var txContainerFirst = docFirst.GetElementbyId("tx_container");
                    var dateFirst = DateTime.Parse(txContainerFirst.Descendants().First(x => x.Name == "span").InnerText);
                    var URLLast = $"https://www.blockchain.com/btc/address/{address}?sort=0";
                    var webLast = new HtmlWeb();
                    var docLast = webLast.Load(URLLast);
                    var txContainerLast = docLast.GetElementbyId("tx_container");
                    var dateLast = DateTime.Parse(txContainerLast.Descendants().First(x => x.Name == "span").InnerText);
                    sb.AppendLine($"{address}\t{dateFirst}\t{dateLast}");
                }
                catch
                {
                    try
                    {
                        Thread.Sleep(25);
                        var URLFirst = $"https://www.blockchain.com/btc/address/{address}?sort=1";
                        var webFirst = new HtmlWeb();
                        var docFirst = webFirst.Load(URLFirst);
                        var txContainerFirst = docFirst.GetElementbyId("tx_container");
                        var dateFirst = DateTime.Parse(txContainerFirst.Descendants().First(x => x.Name == "span").InnerText);
                        var URLLast = $"https://www.blockchain.com/btc/address/{address}?sort=0";
                        var webLast = new HtmlWeb();
                        var docLast = webLast.Load(URLLast);
                        var txContainerLast = docLast.GetElementbyId("tx_container");
                        var dateLast = DateTime.Parse(txContainerLast.Descendants().First(x => x.Name == "span").InnerText);
                        sb.AppendLine($"{address}\t{dateFirst}\t{dateLast}");
                    }
                    catch
                    {
                        Console.WriteLine($"Error on {address}");
                        File.AppendAllText("error.txt", address + "\n");
                    }
                }
            }
            File.WriteAllText("timeline.txt", sb.ToString());
        }
        static void Parse()
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
                var entries = tbody.ChildNodes.Where(x => x.GetType() != typeof(HtmlTextNode));
                containsData = entries.Count() > 0;
                foreach (var line in entries)
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
        }
    }
}
