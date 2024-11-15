package cis5550.tools;

import java.util.*;
import java.net.*;
import java.io.*;

public class URLParser {
  /**
   * Parse a URL into its components
   * Result[0] = protocol
   * Result[1] = host
   * Result[2] = port
   * Result[3] = path
   * @param url
   * @return
   */
  public static String[] parseURL(String url) {
    String result[] = new String[4];
    int slashslash = url.indexOf("//");
    if (slashslash>0) {
      result[0] = url.substring(0, slashslash-1);
      int nextslash = url.indexOf('/', slashslash+2);
      if (nextslash>=0) {
        result[1] = url.substring(slashslash+2, nextslash);
        result[3] = url.substring(nextslash);
      } else {
        result[1] = url.substring(slashslash+2);
        result[3] = "/";
      }
      int colonPos = result[1].indexOf(':');
      if (colonPos > 0) {
        result[2] = result[1].substring(colonPos+1);
        result[1] = result[1].substring(0, colonPos);
      }
    } else {
      result[3] = url;
    }

    return result;
  }

  public static void main(String[] args) {
    String url = "http://www.google.com";
    String[] result = parseURL(url);
    System.out.println("Protocol: " + result[0]);
    System.out.println("Host: " + result[1]);
    System.out.println("Port: " + result[2]);
    System.out.println("Path: " + result[3]);
  }
}