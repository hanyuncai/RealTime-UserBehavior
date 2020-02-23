import java.util.ArrayList;

public class test {
    public static void main(String[] args) {
        ArrayList<String> arr = new ArrayList<String>();
        arr.add("1");
        arr.add("2");
        arr.add("3");
        ArrayList<String> strings = new ArrayList<>();
        strings.add("1");
        strings.add("2");
        //System.out.println(arr.contains("1"));
        System.out.println(arr.containsAll(strings));

    }
}
