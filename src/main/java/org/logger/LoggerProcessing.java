package org.logger;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Stream;

public class LoggerProcessing
{
    private static final String SOURCE_PATH = "logs";
    private static final String DEST_PATH = "logs/transactions_by_users";

    public static void main( String[] args ) throws IOException {
        cleanDestinationFolder();
        Path logsFolder = Paths.get(SOURCE_PATH);
        try(Stream<Path> files = Files.list(logsFolder)) {
            files.filter(path -> path.toString().endsWith(".log")).
                    forEach(path -> {
                try {
                    filesProcessing(path);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        Path destFolder = Paths.get(DEST_PATH);
        try(Stream<Path> files = Files.list(destFolder)) {
            files.forEach(path -> {
                try {
                    sort(path);
                    summary(path);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    public static void summary(Path path) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(path)){
            String line = reader.readLine();
            String user = null;
            double total = 0;
            while (line != null){
                String[] parts = line.split(" ");
                user = parts[2];
                if (parts[3].equals("balance")){
                    total = Double.parseDouble(parts[5]);
                }
                else if (parts[3].equals("transferred") || parts[3].equals("withdraw")){
                    total -= Double.parseDouble(parts[4]);
                }
                else {
                    total += Double.parseDouble(parts[4]);
                }
                line = reader.readLine();
            }
            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            String text = dtf.format( LocalDateTime.now() );
            String totalLine = "[" + text + "]" + " " + user + " final balance " + String.format("%.2f", total) + "\n";
            Files.write(path,totalLine.getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
        }
    }
    public static void sort(Path path) throws IOException {
        ArrayList<String> strings = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(path)){
            String line = reader.readLine();
            while (line != null){
                strings.add(line);
                line = reader.readLine();
            }
            Collections.sort(strings);
        }
        try (BufferedWriter writer = Files.newBufferedWriter(path,StandardCharsets.UTF_8)){
            for (String string : strings) {
                writer.write(string + "\n");
            }
        }
    }
    private static void cleanDestinationFolder() throws IOException {
        Path destPath = Paths.get(DEST_PATH);
        if (!Files.exists(destPath)) {
            Files.createDirectories(destPath);
            return;
        }
        try (Stream<Path> files = Files.list(destPath)) {
            files.forEach(file -> {
                try {
                    Files.delete(file);
                } catch (IOException e) {
                    throw new RuntimeException("Не удалось удалить файл: " + file, e);
                }
            });
        }
    }
    public static void filesProcessing(Path path) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(path)){
            String line = reader.readLine();
            while (line != null){
                String[] parts = line.split(" ");
                String userFrom = parts[2];
                Path userPath = Paths.get(DEST_PATH + "/" + userFrom + ".log");
                Files.write(userPath, (line + "\n").getBytes(StandardCharsets.UTF_8),StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                if (line.contains("transferred")){
                    String date = parts[0];
                    String time = parts[1];
                    String userTo = parts[parts.length - 1];
                    String money = parts[4];
                    Path toUserPath = Paths.get(DEST_PATH + "/" + userTo + ".log");
                    String toLine = date + " " + time + " " + userTo + " received " + money + " from " + userFrom + "\n";
                    Files.write(toUserPath, toLine.getBytes(StandardCharsets.UTF_8),StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                }
                line = reader.readLine();
            }
        }
    }
}
