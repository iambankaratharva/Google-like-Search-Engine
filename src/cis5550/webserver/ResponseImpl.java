package cis5550.webserver;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class ResponseImpl implements Response {
    private int statusCode = 200;
    private String reasonPhrase = "OK";
    private byte[] bodyByte = null;
    private byte[] bodyStringAsByte = null;
    private Map<String, List <String>> headers = new ConcurrentHashMap<>();
    private OutputStream out;
    private String httpVersion;
    private boolean writeFlag = false;
    private boolean halted = false;
    private boolean bodyStringAsByteFlag = false;
    private boolean bodyAsBytes = false;

    ResponseImpl (String httpVersion, OutputStream out) {
        this.httpVersion = httpVersion;
        this.out = out;
    }

    @Override
    public void body(String body) {
        this.bodyStringAsByte = body.getBytes();
        this.bodyStringAsByteFlag = true;
        this.bodyAsBytes = false;
    }

    @Override
    public void bodyAsBytes(byte bodyArg[]) {
        this.bodyByte = bodyArg;
        this.bodyStringAsByteFlag = false;
        this.bodyAsBytes = true;
    }

    @Override
    public void header(String name, String value) {
        // some headers like Cookie can have multiple values
        headers.computeIfAbsent(name, k -> new ArrayList < > ()).add(value);
    }

    @Override
    public void type(String contentType) {
        header("Content-Type", contentType);
    }

    @Override
    public void status(int statusCode, String reasonPhrase) {
        this.statusCode = statusCode;
        this.reasonPhrase = reasonPhrase;
    }

    public void write(byte[] b) throws Exception {
        if (!writeFlag) {
            String responseStatusLine = httpVersion + " " + statusCode + " " + reasonPhrase + "\r\n";
            header("Connection", "close");
            out.write(responseStatusLine.getBytes());

            for (String headerField : fetchHeaders().keySet()) {
                for (String headerValues : fetchHeaders().get(headerField)) {
                    String responseHeaderLine = headerField + ": " + headerValues + "\r\n";
                    out.write(responseHeaderLine.getBytes());
                }
            }
            
            out.write("\r\n".getBytes());
            this.writeFlag = true;
        }

        if (b != null) {
            out.write(b);
            out.flush();
        }
    }

    public void redirect(String url, int responseCode) {
        if (responseCode < 300 || responseCode > 399) {
            System.err.println("Invalid response code for Redirection");
        }
        header("Location", url);
        switch (responseCode) {
            case 301:
                status(301, "Moved Permanently");
                break;
            case 302:
                status(302, "Found");
                break;
            case 303:
                status(303, "See Other");
                break;
            case 307:
                status(307, "Temporary Redirect");
                break;
            case 308:
                status(308, "Permanent Redirect");
                break;
            default:
                status(302, "Found");
                break;
        }
    }

    public void halt(int statusCode, String reasonPhrase) {
        if (!halted) {
            try {
                String response = httpVersion + " " + statusCode + " " + reasonPhrase + "\r\n";
                response += "Content-Length: " + reasonPhrase.length() + "\r\n";
                response += "Content-Type: " + "text/plain" + "\r\n\r\n";
                response += reasonPhrase;
                out.write(response.getBytes());
                out.flush();
                halted = true;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public byte[] fetchBody() {
        if (this.bodyAsBytes == true && this.bodyStringAsByteFlag == false) {
            return this.bodyByte;
        } else if (this.bodyAsBytes == false && this.bodyStringAsByteFlag == true) {
            return this.bodyStringAsByte;
        } else {
            return this.bodyByte;
        }
    }

    public int fetchStatusCode() {
        return statusCode;
    }

    public String fetchReasonPhrase() {
        return reasonPhrase;
    }

    public Map < String, List < String >> fetchHeaders() {
        return headers;
    }

    public boolean fetchWriteStatus() {
        return writeFlag;
    }

    public boolean fetchHaltStatus() {
        return halted;
    }
}
