package ru.vvdolgov.stackoverflow.mapreduce;

import java.util.HashMap;
import java.util.Map;

public class SimpleXmlParser {
    //  <row Id="8" Reputation="947" CreationDate="2008-07-31T21:33:24.057" DisplayName="Eggs McLaren"
    // LastAccessDate="2012-10-15T22:00:45.510" WebsiteUrl="" Location="" AboutMe="&lt;p&gt;This is a puppet
    // test account." Views="5163" UpVotes="12" DownVotes="9" AccountId="6" />

    private static final String QUOTE_SYMBOL = "\"";
    private static final String LEFT_ANGLE_BRACKET_STRING = "&lt;";
    private static final String RIGHT_ANGLE_BRACKET_STRING = "&gt;";
    private static final String EMPTY_STRING_SYMBOL = "";
    private static final String SEMICOLON_SYMBOL = ";";


    public static Map<String, String> parseXmlRow(String row) {
        Map<String, String> map = new HashMap<>();
        try {
            String trimmedRow = row.trim();
            String[] tokens = trimmedRow.substring(5, trimmedRow.length() - 3).split(QUOTE_SYMBOL);

            for (int i = 0; i < tokens.length - 1; i += 2) {
                String key = tokens[i].trim();
                String val = tokens[i + 1];

                map.put(key.substring(0, key.length() - 1), val);
            }
        }
        catch(StringIndexOutOfBoundsException e){
            System.err.println(row);
        }

        return map;
    }

    public static String[] parseMultipleAttributeValues(String attributeValue){
        return attributeValue.replace(RIGHT_ANGLE_BRACKET_STRING+LEFT_ANGLE_BRACKET_STRING, SEMICOLON_SYMBOL)
                .replace(LEFT_ANGLE_BRACKET_STRING, EMPTY_STRING_SYMBOL)
                .replace(RIGHT_ANGLE_BRACKET_STRING, EMPTY_STRING_SYMBOL)
                .split(SEMICOLON_SYMBOL);
    }
}
