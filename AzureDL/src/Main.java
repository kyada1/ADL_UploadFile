import com.microsoft.azure.CloudException;
import com.microsoft.azure.credentials.ApplicationTokenCredentials;
import com.microsoft.azure.management.datalake.store.implementation.DataLakeStoreAccountManagementClientImpl;
import com.microsoft.azure.management.datalake.store.implementation.DataLakeStoreFileSystemManagementClientImpl;
import com.microsoft.azure.management.datalake.store.models.*;
import com.microsoft.azure.management.datalake.store.uploader.*;
import com.microsoft.rest.credentials.ServiceClientCredentials;
import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class Main {
    final static String ADLS_ACCOUNT_NAME = "";
    final static String RESOURCE_GROUP_NAME = "";
    final static String LOCATION = "";
    final static String TENANT_ID = "";
    final static String SUBSCRIPTION_ID =  "";
    final static String CLIENT_ID = "";
    final static String CLIENT_SECRET = ""; // TODO: For production scenarios, we recommend that you replace this line with a more secure way of acquiring the application client secret, rather than hard-coding it in the source code.

    private static DataLakeStoreAccountManagementClientImpl _adlsClient;
    private static DataLakeStoreFileSystemManagementClientImpl _adlsFileSystemClient;

    public static void main(String[] args) throws Exception {
        String localFolderPath = "E:\\"; // TODO: Change this to any unused, new, empty folder on your local machine.

        // Authenticate
        ApplicationTokenCredentials creds = new ApplicationTokenCredentials(CLIENT_ID, TENANT_ID, CLIENT_SECRET, null);
        SetupClients(creds);

        WaitForNewline("", "Displaying account(s).");

        // List Data Lake Store accounts that this app can access
        System.out.println(String.format("All ADL Store accounts that this app can access in subscription %s:", SUBSCRIPTION_ID));
        List<DataLakeStoreAccount> adlsListResult = _adlsClient.accounts().list().getBody();
        for (DataLakeStoreAccount acct : adlsListResult) {
            System.out.println(acct.name());
        }
        WaitForNewline("Account(s) displayed.", "Uploading file.");

        // Upload a file to Data Lake Store: file1.csv
        UploadFile(localFolderPath + "test.txt", "/kumko09022016/test.txt");
        WaitForNewline("File uploaded.", "Appending newline.");

    }

    //Set up clients
    public static void SetupClients(ServiceClientCredentials creds)
    {
        _adlsClient = new DataLakeStoreAccountManagementClientImpl(creds);
        _adlsFileSystemClient = new DataLakeStoreFileSystemManagementClientImpl(creds);
        _adlsClient.withSubscriptionId(SUBSCRIPTION_ID);
    }

    // Helper function to show status and wait for user input
    public static void WaitForNewline(String reason, String nextAction)
    {
        if (nextAction == null)
            nextAction = "";
        if (!nextAction.isEmpty())
        {
            System.out.println(reason + "\r\nPress ENTER to continue...");
            try{System.in.read();}
            catch(Exception e){}
            System.out.println(nextAction);
        }
        else
        {
            System.out.println(reason + "\r\nPress ENTER to continue...");
            try{System.in.read();}
            catch(Exception e){}
        }
    }

    // Create Data Lake Store account
    public static void CreateAccount() throws InterruptedException, CloudException, IOException {
        DataLakeStoreAccount adlsParameters = new DataLakeStoreAccount();
        adlsParameters.withLocation(LOCATION);

        _adlsClient.accounts().create(RESOURCE_GROUP_NAME, ADLS_ACCOUNT_NAME, adlsParameters);
    }

    // Create file
    public static void CreateFile(String path) throws IOException, AdlsErrorException {
        _adlsFileSystemClient.fileSystems().create(ADLS_ACCOUNT_NAME, path);
    }

    // Create file with contents
    public static void CreateFile(String path, String contents, boolean force) throws IOException, AdlsErrorException {
        byte[] bytesContents = contents.getBytes();

        _adlsFileSystemClient.fileSystems().create(ADLS_ACCOUNT_NAME, path, bytesContents, force);
    }

    // Append to file
    public static void AppendToFile(String path, String contents) throws IOException, AdlsErrorException {
        byte[] bytesContents = contents.getBytes();

        _adlsFileSystemClient.fileSystems().append(ADLS_ACCOUNT_NAME, path, bytesContents);
    }

    // Concatenate files
    public static void ConcatenateFiles(List<String> srcFilePaths, String destFilePath) throws IOException, AdlsErrorException {
        _adlsFileSystemClient.fileSystems().concat(ADLS_ACCOUNT_NAME, destFilePath, srcFilePaths);
    }

    // Delete concatenated file
    public static void DeleteFile(String filePath) throws IOException, AdlsErrorException {
        _adlsFileSystemClient.fileSystems().delete(ADLS_ACCOUNT_NAME, filePath);
    }

    // Get file or directory info
    public static FileStatusProperties GetItemInfo(String path) throws IOException, AdlsErrorException {
        return _adlsFileSystemClient.fileSystems().getFileStatus(ADLS_ACCOUNT_NAME, path).getBody().fileStatus();
    }

    // List files and directories
    public static List<FileStatusProperties> ListItems(String directoryPath) throws IOException, AdlsErrorException {
        return _adlsFileSystemClient.fileSystems().listFileStatus(ADLS_ACCOUNT_NAME, directoryPath).getBody().fileStatuses().fileStatus();
    }

    // Upload file
    public static void UploadFile(String srcPath, String destPath) throws Exception {
        UploadParameters parameters = new UploadParameters(srcPath, destPath, ADLS_ACCOUNT_NAME);
        FrontEndAdapter frontend = new DataLakeStoreFrontEndAdapterImpl(ADLS_ACCOUNT_NAME, _adlsFileSystemClient);
        DataLakeStoreUploader uploader = new DataLakeStoreUploader(parameters, frontend);
        uploader.execute();
    }

    // Download file
    public static void DownloadFile(String srcPath, String destPath) throws IOException, AdlsErrorException {
        InputStream stream = _adlsFileSystemClient.fileSystems().open(ADLS_ACCOUNT_NAME, srcPath).getBody();

        PrintWriter pWriter = new PrintWriter(destPath, Charset.defaultCharset().name());

        String fileContents = "";
        if (stream != null) {
            Writer writer = new StringWriter();

            char[] buffer = new char[1024];
            try {
                Reader reader = new BufferedReader(
                        new InputStreamReader(stream, "UTF-8"));
                int n;
                while ((n = reader.read(buffer)) != -1) {
                    writer.write(buffer, 0, n);
                }
            } finally {
                stream.close();
            }
            fileContents =  writer.toString();
        }

        pWriter.println(fileContents);
        pWriter.close();
    }

    // Delete account
    public static void DeleteAccount() throws InterruptedException, CloudException, IOException {
        _adlsClient.accounts().delete(RESOURCE_GROUP_NAME, ADLS_ACCOUNT_NAME);
    }
}