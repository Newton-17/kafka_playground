import java.util.Arrays;

public class logParser {
    public static String[] parseAccessLogs(String logLine){
        String ip = null;
        String timestamp = null;
        String method = null;
        String ext = null;
        String serverCode = null;

        timestamp= logLine.substring(logLine.indexOf("[")+1, logLine.indexOf("]"));
        String[] method_ext = logLine.split("\"");
        method = method_ext[1].split(" ")[0];
        ext = method_ext[1].split(" ")[1];

        String[] logLineArr = logLine.split(" ");
        ip = logLineArr[0];
        serverCode = logLineArr[8];

        System.out.println(ip + " : " + timestamp + " : " + method + " : " + ext + " : " + serverCode);
        String[] returnArr = {ip, timestamp, method, ext, serverCode};

        return returnArr;

    }
}
