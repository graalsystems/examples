package graal.systems.examples;


import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
public class FlinkLocalData {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<String, String, String>> data = env.fromElements(
                new Tuple3<>("Reed", "United States" ,"Female"),
                new Tuple3<>("Bradley", "United States" ,"Female"),
                new Tuple3<>("Adams", "United States" ,"Male"),
                new Tuple3<>("Lane", "United States" ,"Male"),
                new Tuple3<>("Marshall", "United States" ,"Female"),
                new Tuple3<>("Garza", "United States" ,"Male"),
                new Tuple3<>("Gutierrez", "United States" ,"Male"),
                new Tuple3<>("Fox", "Germany" ,"Female"),
                new Tuple3<>("Medina", "United States" ,"Male"),
                new Tuple3<>("Nichols", "United States" ,"Male"),
                new Tuple3<>("Woods", "United States" ,"Male"),
                new Tuple3<>("Welch", "United States" ,"Female"),
                new Tuple3<>("Burke", "United States" ,"Female"),
                new Tuple3<>("Russell", "United States" ,"Female"),
                new Tuple3<>("Burton", "United States" ,"Male"));

        System.out.println("There are " + data.count() + " persons");

        long nb_male = data.filter(new MaleFilter()).count();
        long nb_female = data.filter(new FemaleFilter()).count();
        long nb_americans = data.filter(new AmericanFilter()).count();

        System.out.println("There are " + nb_male + " man");
        System.out.println("There are " + nb_female + " women");
        System.out.println("There are " + nb_americans + " americans");

    }

    public static class MaleFilter implements FilterFunction<Tuple3<String, String, String>> {
        @Override
        public boolean filter(Tuple3<String, String, String> person) {
            return person.f2.equals("Male");
        }
    }

    public static class FemaleFilter implements FilterFunction<Tuple3<String, String, String>> {
        @Override
        public boolean filter(Tuple3<String, String, String> person) {
            return person.f2.equals("Female");
        }
    }

    public static class AmericanFilter implements FilterFunction<Tuple3<String, String, String>> {
        @Override
        public boolean filter(Tuple3<String, String, String> person) {
            return person.f1.equals("United States");
        }
    }
}
