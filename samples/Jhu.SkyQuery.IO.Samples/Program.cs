using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using Jhu.Graywulf.Schema;
using Jhu.Graywulf.Schema.SqlServer;
using Jhu.Graywulf.Format;
using Jhu.Graywulf.IO;
using Jhu.Graywulf.IO.Tasks;
using Jhu.SkyQuery.Format;
using Jhu.SkyQuery.IO;

namespace Jhu.SkyQuery.IO.Samples
{
    class Program
    {
        static void Main(string[] args)
        {
            ExportSample();
            ExportSample_Fits();

            Console.ReadLine();
        }

        static void ExportSample()
        {
            // Create a dataset that wraps mydb
            var mydb = new SqlServerDataset("MYDB", GetConnectionString());

            // Create a file factory that initializes output files
            var ff = FileFormatFactory.Create(null);

            // Create a stream factory that can create you the zip stream, etc.
            var sf = StreamFactory.Create(null);

            // Create a set of sources (tables) to export
            var sources = new SourceTableQuery[]
            {
                SourceTableQuery.Create(mydb.Tables[mydb.DatabaseName, "dbo", "TestData"]),
            };

            // Create the same number of destinations (files) that will
            // be the entries within the archive.
            var destinations = new DataFileBase[]
            {
                ff.CreateFile("TestData.csv"),
            };

            // Initialize the export task
            var task = new ExportTableArchive()
            {
                Uri = Jhu.Graywulf.Util.UriConverter.FromFilePath("export.zip"),
                Sources = sources,
                Destinations = destinations
            };

            // Now you can just run it, the file will be automatically created.
            // If you need to write to an existing stream, you can just wrap it into
            // an archive using the stream factory and pass it to the task the
            // following way:

            // var zipstream = sf.Open(mystream, DataFileMode.Write, DataFileCompression.None, DataFileArchival.Zip);
            // task.Open(zipstream);

            // Run the task
            task.Open();
            task.Execute();
            task.Close();

            // You can also get and print the results:
            foreach (var result in task.Results)
            {
                Console.WriteLine("{0} > {1} ({2} rows)", result.TableName, result.FileName, result.RecordsAffected);
            }
        }

        static void ExportSample_Fits()
        {
            // Create a dataset that wraps mydb
            var mydb = new SqlServerDataset("MYDB", GetConnectionString());

            // To get access to FITS, you need to create a SkyQuery file format factory
            var ff = FileFormatFactory.Create(typeof(SkyQueryFileFormatFactory).AssemblyQualifiedName);

            // Create a stream factory that can create you the zip stream, etc.
            var sf = StreamFactory.Create(null);

            // Create a set of sources (tables) to export
            var sources = new SourceTableQuery[]
            {
                SourceTableQuery.Create(mydb.Tables[mydb.DatabaseName, "dbo", "TestData"]),
            };

            // Create the same number of destinations (files) that will
            // be the entries within the archive.
            var destinations = new DataFileBase[]
            {
                ff.CreateFile("TestData.fits"),
            };

            // Initialize the export task
            var task = new ExportTableArchive()
            {
                Uri = Jhu.Graywulf.Util.UriConverter.FromFilePath("export_fits.zip"),
                Sources = sources,
                Destinations = destinations,
                FileFormatFactoryType = typeof(SkyQueryFileFormatFactory).AssemblyQualifiedName
            };

            // Now you can just run it, the file will be automatically created.
            // If you need to write to an existing stream, you can just wrap it into
            // an archive using the stream factory and pass it to the task the
            // following way:

            // var zipstream = sf.Open(mystream, DataFileMode.Write, DataFileCompression.None, DataFileArchival.Zip);
            // task.Open(zipstream);

            // Run the task
            task.Open();
            task.Execute();
            task.Close();

            // You can also get and print the results:
            foreach (var result in task.Results)
            {
                Console.WriteLine("{0} > {1} ({2} rows)", result.TableName, result.FileName, result.RecordsAffected);
            }
        }


        static string GetConnectionString()
        {
            var csb = new SqlConnectionStringBuilder()
            {
                DataSource = "localhost",
                InitialCatalog = "MYDB_Test",
                IntegratedSecurity = true,
            };

            return csb.ToString();
        }
    }
}
