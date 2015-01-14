using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;

// TODO: rename this app to S3up.exe?
namespace S3Ldr
{
    [Flags]
    public enum RemoteStatus { Unknown = 0, Missing = 1, Same = 2, Different = 4, Compressed = 8 };

    class ContentInfo
    {
        public string OriginalPath { get; set; }
        public FileInfo Info { get; set; }
        public bool IsCompressed { get; set; }
        public string ContentType { get; set; }
    }

    class Program
    {
        internal class Settings
        {
            internal Settings(string[] args)
            {
                Folder = System.Environment.CurrentDirectory;
                Bucket = args[0];

                Direction = Settings.TxDirection.Upload;

                Watch = args.Any(a => string.Compare(a, "/watch", StringComparison.OrdinalIgnoreCase) == 0);
                Clear = args.Any(a => string.Compare(a, "/clear", StringComparison.OrdinalIgnoreCase) == 0);

                IgnoreList = new List<Regex>() { new Regex("s3ldr.ini") };
                MinList = new List<Regex>() { new Regex(@"\.(html|css|js|json|csv|svg|xml)$") };
                GzipList = new List<Regex>() { new Regex(@"\.(html|css|js|csv|txt|log)$") };

                using (var md5 = MD5.Create())
                {
                    var cdhash = string.Join("",
                        md5.ComputeHash(Encoding.UTF8.GetBytes(Folder)).Select(b => b.ToString("X2")));
                    TempFolder = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "S3Ldr", cdhash);
                }
            }

            [Flags]
            public enum TxDirection
            {
                Upload = 1,
                Download = 2,
                Clean = 4
            };

            public string Bucket = "";
            public string Folder = ".";

            public TxDirection Direction { get; set; }

            public bool Watch { get; private set; }
            public bool Clear { get; private set; }

            public List<Regex> IgnoreList { get; private set; }
            public List<Regex> GzipList { get; private set; }
            public List<Regex> MinList { get; private set; }

            public string TempFolder { get; private set; }

            [Flags]
            enum Failures
            {
                None = 0,
                [Description("A valid bucket name must be provided.")]
                BucketNotProvided,
                [Description("A valid local direction must be provided (default is the current directory).")]
                DirectoryNotProvided
            }
            Failures Fail = Failures.None;

            public bool IsValid
            {
                get
                {
                    if (string.IsNullOrWhiteSpace(Bucket))
                        Fail |= Failures.BucketNotProvided;
                    if (string.IsNullOrWhiteSpace(Folder))
                        Fail |= Failures.DirectoryNotProvided;
                    return Fail == Failures.None;
                }
            }

            public void Usage()
            {
                var errors = Enum.GetValues(typeof(Failures)).Cast<Failures>()
                    .Where(v => v != Failures.None && Fail.HasFlag(v))
                    .Select(v => v.ToString())
                    .Select(s => typeof(Failures).GetField(s))
                    .Select(f => f.GetCustomAttributes(typeof(DescriptionAttribute), false))
                    .Where(l => l.Any())
                    .Select(l => (DescriptionAttribute)l.First())
                    .Select(a => a.Description);
                if (errors.Any())
                {
                    Console.WriteLine("It appears that the command-line arguments resulted in some invalid settings:\n");
                    foreach (var e in errors)
                        Console.WriteLine(" - " + e);
                    Console.WriteLine("\nPlease correct the command arguments and try again.");
                }
            }
        }

        static Settings settings = null;

        public static void Main(string[] args)
        {
            settings = new Settings(args);
            if (settings.IsValid)
            {
                var info = Directory.EnumerateFiles(settings.Folder, "*.*", SearchOption.AllDirectories);
                var uploads = Pipeline(info);
                Console.WriteLine(string.Format("{0}: Initial uploads took {1} seconds total.", DateTime.Now, uploads.Sum(t => t.TotalSeconds)));

                if (settings.Watch)
                {
                    Console.WriteLine(string.Format("{0}: Now watching for changes in {1}. Hit any key to exit.", DateTime.Now, settings.Folder));

                    var dictionaries = new
                    {
                        up = new ConcurrentDictionary<string, DateTime>(),
                        del = new ConcurrentDictionary<string, DateTime>()
                    };

                    var e = new System.Threading.AutoResetEvent(false);
                    Task.Run(async () =>
                    {
                        while (true)
                        {
                            var next = dictionaries.up.Where(kv => kv.Value < DateTime.Now);
                            foreach (var kv in next)
                            {
                                Task.Run(() =>
                                {
                                    var t = DateTime.MinValue;
                                    if (dictionaries.up.TryRemove(kv.Key, out t))
                                        Pipeline(new[] { kv.Key }).ToArray();
                                });
                            }
                            if (!next.Any()) await Task.Delay(500);
                        }
                        Console.WriteLine("{0}: Exiting watch loop.", DateTime.Now);
                    });

                    using (var w = new FileSystemWatcher(settings.Folder) { IncludeSubdirectories = true })
                    {
                        w.Created += (sender, arg) =>
                        {
                            dictionaries.up.AddOrUpdate(arg.FullPath, DateTime.Now.AddSeconds(2), (s, d) => DateTime.Now.AddSeconds(2));
                            Console.WriteLine(string.Format("{0}: Observed creation of {1} .", DateTime.Now, arg.FullPath));
                        };
                        w.Changed += (sender, arg) =>
                        {
                            dictionaries.up.AddOrUpdate(arg.FullPath, DateTime.Now.AddSeconds(2), (s, d) => DateTime.Now.AddSeconds(2));
                            Console.WriteLine(string.Format("{0}: Observed change of {1} .", DateTime.Now, arg.FullPath));
                        };
                        w.Deleted += (sender, arg) =>
                        {
                            if (settings.Clear) dictionaries.del.AddOrUpdate(arg.FullPath, DateTime.Now.AddSeconds(2), (s, d) => DateTime.Now.AddSeconds(2));
                            else Console.WriteLine(string.Format("{0}: File \"{1}\" removed locally but will remain on s3.  Use /clear to enable removal of remote files.", DateTime.Now, arg.Name));
                        };
                        w.Error += (sender, arg) =>
                        {
                            throw new Exception("Exception occurred in file watcher: {0}", arg.GetException());
                        };
                        w.EnableRaisingEvents = true;

                        Console.Read();
                    }
                }
            }
            else settings.Usage();
        }

        static ParallelQuery<TimeSpan> Pipeline(IEnumerable<string> paths)
        {
            var info = paths.AsParallel()
                .Select(path => new { path = path.Remove(0, settings.Folder.Length + 1), attributes = File.GetAttributes(path) });

            var filtered = info
                .Where(a => a.attributes.HasFlag(FileAttributes.Archive))
                .Where(a => !settings.IgnoreList.Any(ex => ex.IsMatch(a.path)));

            var prepared = filtered
                .Select(a => new { a.path, content = Content(a.path) })
                .Select(a => new { a.path, a.content, key = Key(a.path), hash = Hash(a.content.Info.FullName) }) // local calculations
                .Select(a => new { a.path, a.content, a.key, a.hash, status = S3Status(a.key, a.hash) }); // remote status

            var uploads = prepared
                .Where(a => !a.status.HasFlag(RemoteStatus.Same))
                .Select(a => DoUpload(a.content, a.key));

            return uploads;
        }

        static ContentInfo Content(string path)
        {
            var content = path;
            var gzip = false;

            if (settings.MinList.Any(m => m.IsMatch(path)))
            {
                var min = Path.Combine(settings.TempFolder, "Minify", path);
                if (!Directory.Exists(min))
                    Directory.CreateDirectory(Path.GetDirectoryName(min));
                if (File.Exists(min))
                    File.Delete(min);
                Minify(content, min);
                content = min;
            }
            if (settings.GzipList.Any(z => z.IsMatch(path)))
            {
                var zip = Path.Combine(settings.TempFolder, "Gzip", path);
                if (!Directory.Exists(zip))
                    Directory.CreateDirectory(Path.GetDirectoryName(zip));
                if (File.Exists(zip))
                    File.Delete(zip);
                Compress(content, zip);
                content = zip;
                gzip = true;
            }
            return new ContentInfo()
            {
                OriginalPath = path,
                Info = new FileInfo(content),
                IsCompressed = gzip
            };
        }

        private static readonly Regex RegexBetweenTags = new Regex(@">(?! )\s+", RegexOptions.Compiled);
        private static readonly Regex RegexLineBreaks = new Regex(@"([\n\s])+?(?<= {2,})<", RegexOptions.Compiled);
        private static void Minify(string from, string to)
        {
            var m = new Microsoft.Ajax.Utilities.Minifier();
            switch (Path.GetExtension(from))
            {
                case ".js":
                    File.WriteAllText(to, m.MinifyJavaScript(from, new Microsoft.Ajax.Utilities.CodeSettings()
                    {
                        PreserveFunctionNames = true
                    }));
                    break;

                case ".json":
                    {
                        var json = File.ReadAllText(from);
                        json = Regex.Replace(json, "(\"(?:[^\"\\\\]|\\\\.)*\")|\\s+", "$1");
                        File.WriteAllText(to, json);
                    } break;

                case ".css":
                    File.WriteAllText(to, m.MinifyStyleSheet(from, new Microsoft.Ajax.Utilities.CssSettings()
                    {
                    }));
                    break;

                case ".htm":
                case ".html":
                    {
                        var html = File.ReadAllText(from);
                        html = RegexBetweenTags.Replace(html, ">");
                        html = RegexLineBreaks.Replace(html, "<");
                        html = html.Trim();
                        File.WriteAllText(to, html);
                    } break;

                default:
                    File.Copy(from, to); // TODO: minify based on content type
                    break;
            }
        }

        private static void DeMinify(string from, string to)
        {
            // .json
            // JsonConvert.SerializeObject(jsonString, Formatting.Indented);
        }

        private static void Compress(string from, string to)
        {
            using (var f = File.OpenRead(from))
            {
                using (var t = File.Create(to))
                {
                    using (var z = new System.IO.Compression.GZipStream(t, System.IO.Compression.CompressionMode.Compress))
                    {
                        f.CopyTo(z);
                    }
                }
            }
        }

        /// <summary>
        ///     Make a S3 compatible key name from a system file path.
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        public static string Key(string path)
        {
            return path.Replace("\\", "/");
        }

        /// <summary>
        ///    Calculate the MD5 hash of the file contents so we can compare it to what's
        ///    held in S3 as the object etag.
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        public static string Hash(string path)
        {
            using (var md5 = MD5.Create())
            {
                using (var f = File.OpenRead(path))
                    return string.Join("", md5.ComputeHash(f).Select(b => b.ToString("X2")));
            }
        }

        static char[] quote = { '"' };
        static TransferUtility tu = new TransferUtility();
        internal static RemoteStatus S3Status(string key, string hash)
        {
            var status = RemoteStatus.Unknown;
            try
            {
                var meta = tu.S3Client.GetObjectMetadata(new GetObjectMetadataRequest()
                {
                    Key = key,
                    BucketName = settings.Bucket
                });

                status = string.Compare(meta.ETag.Trim(quote), hash, StringComparison.OrdinalIgnoreCase) == 0 ?
                    RemoteStatus.Same
                    : RemoteStatus.Different;
                if (meta.Metadata["Content-Encoding"] != null && meta.Metadata["Content-Encoding"].Contains("gzip"))
                    status &= RemoteStatus.Compressed;
            }
            catch (AmazonS3Exception e)
            {
                if (e.ErrorCode == "NotFound")
                    status = RemoteStatus.Missing;
                else throw new Exception("Can't get status from S3", e);
            }
            Console.WriteLine(string.Format("{0}: The key \"{1}\" is {2} on s3.", DateTime.Now, key, status));
            return status;
        }

        internal static TimeSpan DoUpload(ContentInfo content, string key)
        {
            Console.WriteLine(string.Format("{0}: Uploading \"{1}\" to s3.", DateTime.Now, content.OriginalPath));
            var sw = new Stopwatch();
            sw.Start();
            if (content.Info.Length < (1024 * 1024 * 3))
            {
                var r = new TransferUtilityUploadRequest()
                {
                    BucketName = settings.Bucket,
                    Key = key,
                    FilePath = content.Info.FullName,
                    ContentType = content.ContentType
                };
                if (content.IsCompressed)
                    r.Headers.ContentEncoding = "gzip";
                tu.Upload(r);
            }
            else DoMultiPartUpload(content, key);
            return sw.Elapsed;
        }

        /// <summary>
        /// Upload the file in 1MB chunks so that if there is a problem during the upload
        /// only the missing parts will need to be resent.  This method should only be used
        /// with files that are at least three times larger than the part size (i.e. > 3MB)
        /// </summary>
        /// <param name="path"></param>
        /// <param name="key"></param>
        /// <returns></returns>
        internal static void DoMultiPartUpload(ContentInfo content, string key)
        {
            var size = (1024 * 1024); // 1MB parts
            var count = content.Info.Length / size;

            var imr = new InitiateMultipartUploadRequest()
            {
                Key = key,
                BucketName = settings.Bucket,
                CannedACL = S3CannedACL.PublicRead
            };
            if (content.IsCompressed) imr.Headers.ContentEncoding = "gzip";
            var mpu = tu.S3Client.InitiateMultipartUpload(imr);

            var parts = Enumerable.Range(0, (int)count).Select(i => new UploadPartRequest()
            {
                UploadId = mpu.UploadId,
                FilePath = content.Info.FullName,
                BucketName = settings.Bucket,
                Key = key,
                PartNumber = i,
                FilePosition = i * size,
                PartSize = size
            });

            var tasks = parts.Select(async p =>
            {
                var atempt = 1;
                while (atempt <= 3)
                {
                    try
                    {
                        return await tu.S3Client.UploadPartAsync(p);
                    }
                    catch (AmazonS3Exception ex)
                    {
                        Console.WriteLine(string.Format("{0}: Failed attempt #{1} to upload part {2} of file {3} with message: {4}.",
                            DateTime.Now, atempt, p.PartNumber, content.Info.Name, ex.Message));
                        atempt++;
                    }
                    await Task.Delay(2000);
                }
                throw new Exception(string.Format("Failed to upload file {0}.", content.Info.Name));
            }).ToArray();

            Task.WaitAll(tasks);

            var results = tasks.Select(t => t.Result);
            var etags = results.Select(p => new PartETag(p.PartNumber, p.ETag)).ToList();

            var final = tu.S3Client.CompleteMultipartUpload(new CompleteMultipartUploadRequest()
            {
                BucketName = settings.Bucket,
                Key = key,
                PartETags = etags,
                UploadId = mpu.UploadId
            });
        }
    }
}