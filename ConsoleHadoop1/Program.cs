using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using Microsoft.Hadoop.WebHDFS;
namespace ConsoleHadoop1
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                string str = string.Empty;
                if (string.IsNullOrEmpty(str))
                {
                    throw new Exception("String Message Error");
                }
            }
            catch (Exception ex)
            {
                ErrorLogging(ex);
                //ReadError();
                LogToHadoop();
            }

        }

        public static void ErrorLogging(Exception ex)
        {
            string strPath = "C:\\Users\\deepakk\\Desktop\\Log.txt";
            if (!File.Exists(strPath))
            {
                File.Create(strPath).Dispose();
            }
            using (StreamWriter sw = File.AppendText(strPath))
            {
                sw.WriteLine(DateTime.Now + " ===================================== Log Start =================================== ");
                sw.WriteLine("Error Message: " + ex.Message);
                sw.WriteLine("Stack Trace: " + ex.StackTrace);
                sw.WriteLine(DateTime.Now + " ===================================== Log End ===================================== ");

            }
        }

        public static void ReadError()
        {
            string strPath = "C:\\Users\\deepakk\\Desktop\\Log.txt";
            using (StreamReader sr = new StreamReader(strPath))
            {
                string line;
                while ((line = sr.ReadLine()) != null)
                {
                    Console.WriteLine(line);
                }
            }
        }

        public static void LogToHadoop()
        {
            try
            {
                //set variables
                string srcFileName = "C:\\Users\\deepakk\\Desktop\\Log.txt";
                string destFolderName = "/log_error";
                string destFileName = "err2.txt";

                //connect to hadoop cluster
                Uri myUri = new Uri("http://localhost:50070/");
                string userName = "DeepakK";
                WebHDFSClient myClient = new WebHDFSClient(myUri, userName);

                //drop destination directory (if exists)
                myClient.DeleteDirectory(destFolderName, true).Wait();

                //create destination directory
                myClient.CreateDirectory(destFolderName).Wait();


                //load file to destination directory
                myClient.CreateFile(srcFileName, destFolderName + "/" + destFileName).Wait();

                //list file contents of destination directory
                Console.WriteLine();
                Console.WriteLine("Contents of " + destFolderName);

                myClient.GetDirectoryStatus(destFolderName).ContinueWith(
                     ds => ds.Result.Files.ToList().ForEach(
                     f => Console.WriteLine("t" + f.PathSuffix)
                     ));

                //----------------------------------------------------
                //---------------------------------
                //myClient.CreateDirectory("/TEST")
                //.ContinueWith(x => myClient.CreateFile(@"C:\Users\Administrator\Desktop\integer.txt", "/user/hadoop/integer.txt")
                //.ContinueWith(t => Console.WriteLine("new file located at " + t.Result))
                //.ContinueWith(t => myClient.OpenFile("/user/hadoop/integer.txt")
                //.ContinueWith(resp => resp.Result.Content.ReadAsStringAsync()
                //.ContinueWith(bigString => Console.WriteLine("new file is " + bigString.Result.Length + " bytes long"))
                //.ContinueWith(t2 => myClient.DeleteDirectory("/user/Administrator/'demosimplenewin'")
                //    .ContinueWith(b => Console.WriteLine("Successfully deleted file."))
                //                )
                //            )
                //        )
                //    );
                //-----------------------------------------------------

                //keep command window open until user presses enter
                Console.ReadLine();


            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
    }
}