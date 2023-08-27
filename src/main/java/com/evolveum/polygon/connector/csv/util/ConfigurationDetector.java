package com.evolveum.polygon.connector.csv.util;


import com.evolveum.polygon.connector.csv.CsvConnector;
import com.evolveum.polygon.connector.csv.ObjectClassHandlerConfiguration;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.ConnectorIOException;
import org.identityconnectors.framework.common.objects.SuggestedValues;
import org.identityconnectors.framework.common.objects.SuggestedValuesBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class ConfigurationDetector {

    private static final Log LOG = Log.getLog(ConfigurationDetector.class);

    private static final int MAX_PROCESSED_ROW = 1000;

    private final ObjectClassHandlerConfiguration configuration;

    public ConfigurationDetector(ObjectClassHandlerConfiguration configuration) {
        this.configuration = configuration;
    }

    public Map<String, SuggestedValues> detectDelimiters() {

        Map<String, SuggestedValues> suggestions = new HashMap<>();

        try {

            List<Character> characters = readCharacters();

            Set<Character> allSymbols = new HashSet<>();
            Map<Character, SymbolWrapper> symbols = new HashMap<>();
            List<Map<Character, SymbolWrapper>> symbolsPerRow = new ArrayList<>();

            int i;
            for (i = 0; i < characters.size(); i++) {

                char ch = characters.get(i);

                if (isSymbol(ch)) {
                    allSymbols.add(ch);
                    increment(symbols, ch, isFirstInRow(i, characters));
                } else if (isNewLineChar(ch) && symbols.size() > 0) {
                    symbolsPerRow.add(symbols);
                    symbols = new HashMap<>();
                }
            }

            if (symbols.size() > 0 && i == characters.size()) {
                symbolsPerRow.add(symbols);
            }

            Set<Character> isNotInEveryRow = new HashSet<>();

            symbolsPerRow.forEach(currentRow -> {
                allSymbols.forEach(symbol -> {
                    if (!currentRow.containsKey(symbol)) {
                        isNotInEveryRow.add(symbol);
                    }
                });
            });

            Set<Character> commentSuggestions = createCommentSuggestions(isNotInEveryRow, symbolsPerRow);

            removeRowWithComment(symbolsPerRow, commentSuggestions);

            Set<Character> quote = createQuoteSuggestions(allSymbols, symbolsPerRow, commentSuggestions);

            Set<Character> delimiters = createRecordDelimitersSuggestions(symbolsPerRow, allSymbols);

            if (quote.contains('"') && delimiters.contains('"')) {
                delimiters.remove('"');
            }

            if (quote.contains('\'') && delimiters.contains('\'')) {
                delimiters.remove('\'');
            }

            delimiters.forEach(delimiter -> {
                if (quote.contains(delimiter)) {
                    quote.remove(delimiter);
                }
            });

            if (!commentSuggestions.isEmpty()) {
                suggestions.put("commentMarker",
                        SuggestedValuesBuilder.buildOpen(commentSuggestions.toArray()));
            }
            if (!delimiters.isEmpty()) {
                suggestions.put("fieldDelimiter",
                        SuggestedValuesBuilder.buildOpen(delimiters.toArray()));
            }

            Set<Character> multivalueDelimiters = createMultivalueDelimitersSuggestions(characters, delimiters, quote);

            if (!quote.isEmpty()) {
                suggestions.put("quote",
                        SuggestedValuesBuilder.buildOpen(quote.toArray()));
            }
            if (!multivalueDelimiters.isEmpty()) {
                suggestions.put("multivalueDelimiter",
                        SuggestedValuesBuilder.buildOpen(multivalueDelimiters.toArray()));
            }

        } catch (
                IOException e) {
            LOG.error("Couldn't create reader for file: {1} ({2})", configuration.getFilePath().getPath(), e.getMessage());
            throw new ConnectorIOException(e.getMessage(), e);
        }
        return suggestions;
    }

    private Set<Character> createRecordDelimitersSuggestions(List<Map<Character, SymbolWrapper>> symbolsPerRow, Set<Character> allSymbols) {
        Map<Character, Integer> sums = new HashMap<>();
        Set<Character> toRemove = new HashSet<>();

        symbolsPerRow.forEach(previousRow -> {
            symbolsPerRow.forEach((currentRow) -> {
                allSymbols.forEach(symbol -> {
                    SymbolWrapper previousWrapper = previousRow.get(symbol);
                    SymbolWrapper currentWrapper = currentRow.get(symbol);
                    if (previousWrapper == null && currentWrapper == null) {
                        toRemove.add(symbol);
                    }

                    if (previousWrapper != null && currentWrapper != null) {
                        increment(sums, symbol, Math.abs(previousWrapper.countInRow - currentWrapper.countInRow));
                    }
                });
            });
        });

        if (toRemove.size() == sums.size()) {
            Map<Character, Integer> lineCount = new HashMap<Character, Integer>();
            for (int i = 0; i < symbolsPerRow.size(); i++) {
                for (Character symbolInRow : symbolsPerRow.get(i).keySet()) {
                    Integer count = lineCount.get(symbolInRow);
                    if (count == null) {
                        count = 0;
                    }
                    lineCount.put(symbolInRow, count + 1);
                }
            }

            Integer highestLineCount = null;
            for (Map.Entry<Character, Integer> e : lineCount.entrySet()) {
                if (highestLineCount == null || highestLineCount < e.getValue()) {
                    highestLineCount = e.getValue();
                }
            }

            List<Character> bestCandidates = new ArrayList<>();
            for (Map.Entry<Character, Integer> e : lineCount.entrySet()) {
                if (e.getValue().equals(highestLineCount)) {
                    bestCandidates.add(e.getKey());
                }
            }

            if (!bestCandidates.isEmpty()) {
                toRemove.removeAll(bestCandidates);
            }
        }

        sums.keySet().removeAll(toRemove);

        return sums.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (oldValue, newValue) -> oldValue, LinkedHashMap::new)).keySet();
    }

    private Set<Character> createMultivalueDelimitersSuggestions(List<Character> characters, Set<Character> delimiters, Set<Character> quote) {
        Map<Character, Integer> multivalueDelimiterMap = new HashMap<>();
        List<Character> processedDelimiters = new ArrayList<>();
        for (int i = 0; i < characters.size(); i++) {
            char ch = characters.get(i);
            if (delimiters.contains(ch) && i + 1 < characters.size()) {
                if (!processedDelimiters.contains(ch) && i > 0) {
                    for (int j = i - 1; j >= 0; j--) {
                        char prevCh = characters.get(j);
                        if (ch == prevCh || isNewLineChar(prevCh)) {
                            break;
                        }
                        if (isSymbol(prevCh)) {
                            if (!delimiters.contains(prevCh)
                                    && j != 0
                                    && j != (i - 1)
                                    && ch != characters.get(j - 1)
                                    && !isNewLineChar(characters.get(j - 1))) {
                                increment(multivalueDelimiterMap, prevCh, 1);

                            }
                            if (quote.contains(prevCh) && isNotQuote(j, characters)) {
                                quote.remove(prevCh);
                            }
                        }
                    }
                    processedDelimiters.add(ch);
                }
                for (int j = i + 1; j < characters.size(); j++) {
                    char nextCh = characters.get(j);
                    if (ch == nextCh || isNewLineChar(nextCh)) {
                        break;
                    }
                    if (isSymbol(nextCh)) {
                        if (!delimiters.contains(nextCh)
                                && (j + 1) != characters.size()
                                && j != (i + 1)
                                && ch != characters.get(j + 1)
                                && !isNewLineChar(characters.get(j + 1))) {
                            increment(multivalueDelimiterMap, nextCh, 1);

                        }
                        if (quote.contains(nextCh) && isNotQuote(j, characters)) {
                            quote.remove(nextCh);
                        }
                    }
                }
            }
            if (isNewLineChar(ch)) {
                processedDelimiters.clear();
            }
        }
        return multivalueDelimiterMap.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (oldValue, newValue) -> oldValue, LinkedHashMap::new)).keySet();
    }

    private boolean isNotQuote(int j, List<Character> characters) {
        return j != 0 && !isSymbol(characters.get(j - 1)) && !isNewLineChar(characters.get(j - 1))
                && !isSymbol(characters.get(j + 1)) && !isNewLineChar(characters.get(j + 1)) && (j + 1) != characters.size();
    }

    private Set<Character> createQuoteSuggestions(Set<Character> allSymbols, List<Map<Character, SymbolWrapper>> symbolsPerRow, Set<Character> commentSuggestions) {
        Set<Character> quote = new HashSet<>();

        allSymbols.forEach(symbol -> {
            boolean evenInEveryRowEven = true;
            for (Map<Character, SymbolWrapper> row : symbolsPerRow) {
                SymbolWrapper wrapper = row.get(symbol);
                if (commentSuggestions.contains(symbol) || wrapper != null && (wrapper.getCountInRow() % 2) != 0) {
                    evenInEveryRowEven = false;
                }
            }
            if (evenInEveryRowEven) {
                quote.add(symbol);
            }
        });
        return quote;
    }

    private void removeRowWithComment(List<Map<Character, SymbolWrapper>> symbolsPerRow, Set<Character> commentSuggestions) {
        Iterator<Map<Character, SymbolWrapper>> symbolsPerRowIterator = symbolsPerRow.iterator();

        while (symbolsPerRowIterator.hasNext()) {
            Map<Character, SymbolWrapper> row = symbolsPerRowIterator.next();
            commentSuggestions.forEach(commentSymbol -> {
                SymbolWrapper wrapper = row.get(commentSymbol);
                if (wrapper != null && wrapper.isFirstInRow()) {
                    symbolsPerRowIterator.remove();
                }
            });
        }
    }

    private Set<Character> createCommentSuggestions(Set<Character> isNotInEveryRow, List<Map<Character, SymbolWrapper>> symbolsPerRow) {
        Set<Character> commentSuggestions = new HashSet<>();
        isNotInEveryRow.forEach(character -> {
            boolean isFirstInEveryRow = true;
            for (Map<Character, SymbolWrapper> row : symbolsPerRow) {
                SymbolWrapper wrapper = row.get(character);
                if (wrapper != null && (wrapper.countInRow != 1 || !wrapper.isFirstInRow)) {
                    isFirstInEveryRow = false;
                }
            }
            if (isFirstInEveryRow) {
                commentSuggestions.add(character);
            }
        });
        return commentSuggestions;
    }

    private List<Character> readCharacters() throws IOException {
        BufferedReader reader = Util.createReader(configuration);
        int countOfRow = 0;

        List<Character> characters = new ArrayList<>();

        synchronized (CsvConnector.SYNCH_FILE_LOCK) {
            int r;
            boolean previousIsNewLine = false;
            while ((r = reader.read()) != -1
                    && (countOfRow < MAX_PROCESSED_ROW || (countOfRow == MAX_PROCESSED_ROW && previousIsNewLine))) {
                char ch = (char) r;
                if (isNewLineChar(ch)) {
                    if (!previousIsNewLine) {
                        countOfRow++;
                        previousIsNewLine = true;
                    }
                } else {
                    previousIsNewLine = false;
                    if (countOfRow == MAX_PROCESSED_ROW) {
                        continue;
                    }
                }
                characters.add(ch);
            }
            reader.close();
        }
        return characters;
    }

    public Map<String, ? extends SuggestedValues> detectAttributes() {
        Map<String, SuggestedValues> suggestions = new HashMap<>();

        try {
            BufferedReader reader = Util.createReader(configuration);

            String firstLine;

            synchronized (CsvConnector.SYNCH_FILE_LOCK) {
                firstLine = reader.readLine();
                reader.close();
            }

            List<String> attributes = getAttributes(firstLine);

            List<String> passwordAttr = attributes;

            if (!passwordAttr.isEmpty()) {
                suggestions.put("passwordAttribute",
                        SuggestedValuesBuilder.buildOpen(passwordAttr.toArray()));
            }

            List<String> nameAttr = attributes;
            if (!nameAttr.isEmpty()) {
                suggestions.put("nameAttribute",
                        SuggestedValuesBuilder.buildOpen(nameAttr.toArray()));
            }

            List<String> idAttr = attributes;
            if (!idAttr.isEmpty()) {
                suggestions.put("uniqueAttribute",
                        SuggestedValuesBuilder.buildOpen(idAttr.toArray()));
            }

        } catch (IOException e) {
            LOG.error("Couldn't create reader for file: {1} ({2})", configuration.getFilePath().getPath(), e.getMessage());
            throw new ConnectorIOException(e.getMessage(), e);
        }
        return suggestions;
    }

    private List<String> getAttributes(String firstLine) {
        List<String> attributes = new ArrayList<>();
        int startIndex = 0;
        Integer endIndex = null;
        for(int i = 0; i < firstLine.length(); i++) {
            char ch = firstLine.charAt(i);
            if (isSymbol(ch) && ch != '_' && ch != '-') {
                endIndex = i;
            }

            if (endIndex != null) {
                attributes.add(firstLine.substring(startIndex, endIndex));
                startIndex = endIndex + 1;
                endIndex = null;
            }
        }
        attributes.add(firstLine.substring(startIndex));

//        while (firstLine.toLowerCase().contains(attributeName)) {
//            int startIndex = firstLine.toLowerCase().indexOf(attributeName);
//            int endIndex = startIndex + attributeName.length();
//            int i;
//            for (i = startIndex; i >= 0; i--) {
//                char ch = firstLine.charAt(i);
//                if (isSymbol(ch) && ch != '_' && ch != '-') {
//                    break;
//                }
//            }
//            startIndex = i + 1;
//
//            for (i = endIndex; i < firstLine.length(); i++) {
//                char ch = firstLine.charAt(i);
//                if (isSymbol(ch) && ch != '_' && ch != '-') {
//                    break;
//                }
//            }
//            endIndex = i;
//            attributes.add(firstLine.substring(startIndex, endIndex));
//            firstLine = firstLine.substring(endIndex);
//        }
        return attributes;
    }

    private boolean isFirstInRow(int i, List<Character> characters) {
        if (i == 0) {
            return true;
        }
        return isNewLineChar(characters.get(--i));
    }

    private boolean isNewLineChar(char ch) {
        return ch == '\n' || ch == '\r';
    }

    protected boolean isSymbol(char ch) {
        return !Character.isLetterOrDigit(ch) && (ch == '\t' || ch >= ' ');
    }

    protected void increment(Map<Character, SymbolWrapper> map, char symbol, boolean isFirstInRow) {
        this.increment(map, symbol, isFirstInRow, 1);
    }

    protected void increment(Map<Character, SymbolWrapper> map, char symbol, boolean isFirstInRow, int incrementSize) {
        SymbolWrapper wrapper = map.get(symbol);
        if (wrapper == null) {
            wrapper = new SymbolWrapper(isFirstInRow, 0);
        }
        wrapper.incrementCount(incrementSize);
        map.put(symbol, wrapper);
    }

    private void increment(Map<Character, Integer> map, Character symbol, int increment) {
        Integer count = map.get(symbol);
        if (count == null) {
            count = 0;
        }
        map.put(symbol, count + increment);
    }

    private class SymbolWrapper {

        private final boolean isFirstInRow;
        private int countInRow;

        SymbolWrapper(boolean isFirstInRow, int countInRow) {
            this.isFirstInRow = isFirstInRow;
            this.countInRow = countInRow;
        }

        private void incrementCount(int increment) {
            countInRow += increment;
        }

        public boolean isFirstInRow() {
            return isFirstInRow;
        }

        public int getCountInRow() {
            return countInRow;
        }

        @Override
        public boolean equals(Object obj) {
            return super.equals(obj);
        }
    }
}
