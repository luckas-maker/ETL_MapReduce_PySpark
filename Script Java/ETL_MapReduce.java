import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ETL_MapReduce {

    // =====================================================
    // JOB 1: Total por factura  (cantidad * precio)
    // CSV: idFactura,idP,cantidad,precio
    // =====================================================
    public static class TotalFacturaMapper
            extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private final Text clave = new Text();
        private final DoubleWritable monto = new DoubleWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String linea = value.toString().trim();

            if (linea.isEmpty() || linea.startsWith("idFactura")) return;

            String[] campos = linea.split(",");
            if (campos.length < 4) return; // línea incompleta

            try {
                String idFactura = campos[0].trim();
                double cantidad = Double.parseDouble(campos[2].trim());
                double precio   = Double.parseDouble(campos[3].trim());

                clave.set(idFactura);
                monto.set(cantidad * precio);
                context.write(clave, monto);
            } catch (NumberFormatException e) {
                // línea con datos no numéricos: se ignora
            }
        }
    }

    public static class TotalFacturaReducer
            extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private final DoubleWritable total = new DoubleWritable();

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double suma = 0;
            for (DoubleWritable val : values) {
                suma += val.get();
            }
            total.set(suma);
            context.write(key, total);
        }
    }

    // =====================================================
    // JOB 2: JOIN Productos + FacturasDetalle
    // Productos.csv: idP,nombre,...
    // =====================================================
    public static class ProductosMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String linea = value.toString().trim();

            if (linea.isEmpty() || linea.startsWith("idP")) return;

            String[] datos = linea.split(",");
            if (datos.length < 2) return;

            String idP = datos[0].trim();
            String nombre = datos[1].trim();

            context.write(new Text(idP), new Text("P," + nombre));
        }
    }

    public static class DetalleMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String linea = value.toString().trim();

            if (linea.isEmpty() || linea.startsWith("idFactura")) return;

            String[] datos = linea.split(",");
            if (datos.length < 4) return;

            try {
                String idP = datos[1].trim();
                double cantidad = Double.parseDouble(datos[2].trim());
                double precio   = Double.parseDouble(datos[3].trim());

                context.write(new Text(idP), new Text("D," + (cantidad * precio)));
            } catch (NumberFormatException e) {
                // línea inválida: se ignora
            }
        }
    }

    public static class JoinReducer
            extends Reducer<Text, Text, Text, DoubleWritable> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String nombre = "";
            double total = 0;

            for (Text v : values) {
                // limit 2: si el nombre trae comas no se corta
                String[] partes = v.toString().split(",", 2);

                if (partes[0].equals("P")) {
                    nombre = partes[1];
                } else if (partes[0].equals("D")) {
                    total += Double.parseDouble(partes[1]);
                }
            }

            if (!nombre.isEmpty()) {
                context.write(new Text(nombre), new DoubleWritable(total));
            }
        }
    }

    // =====================================================
    // Lee todos los part-r-* de una carpeta de salida de HDFS
    // =====================================================
    private static int cargarSalida(FileSystem fs, Path carpeta, PreparedStatement ps)
            throws Exception {

        int insertados = 0;

        for (FileStatus st : fs.listStatus(carpeta)) {
            String nombreArchivo = st.getPath().getName();
            if (!nombreArchivo.startsWith("part-")) continue; // omite _SUCCESS

            try (FSDataInputStream in = fs.open(st.getPath());
                 BufferedReader br = new BufferedReader(
                         new InputStreamReader(in, StandardCharsets.UTF_8))) {

                String linea;
                while ((linea = br.readLine()) != null) {
                    linea = linea.trim();
                    if (linea.isEmpty()) continue;

                    // Hadoop separa clave y valor con TAB; se usa el último TAB
                    int pos = linea.lastIndexOf('\t');
                    if (pos < 0) continue;

                    String clave = linea.substring(0, pos).trim();
                    double valor = Double.parseDouble(linea.substring(pos + 1).trim());

                    ps.setString(1, clave);
                    ps.setDouble(2, valor);
                    ps.addBatch();
                    insertados++;
                }
            }
        }
        ps.executeBatch();
        return insertados;
    }

    public static void main(String[] args) throws Exception {

        // 1. Configuración MySQL
        String dbUrl  = "jdbc:mysql://localhost:3306/practica32?serverTimezone=America/Mexico_City";
        String dbUser = "root";
        String dbPass = "root";

        // 2. Configuración HDFS
        String hdfsUri = "hdfs://localhost:9000";

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsUri);
        FileSystem fs = FileSystem.get(conf);

        Path dirPath   = new Path("/proyectofinalGD");
        Path detalle   = new Path("/proyectofinalGD/FacturasDetalle.csv");
        Path productos = new Path("/proyectofinalGD/Productos.csv");

        if (!fs.exists(dirPath)) {
            fs.mkdirs(dirPath);
            System.out.println("Directorio creado en HDFS: /proyectofinalGD");
        }

        // Validar que los CSV ya estén en HDFS
        if (!fs.exists(detalle) || !fs.exists(productos)) {
            System.err.println("Faltan archivos en HDFS. Sube FacturasDetalle.csv y "
                    + "Productos.csv a /proyectofinalGD (hdfs dfs -put ...)");
            System.exit(1);
        }

        // 3. JOB 1: total por factura
        Path salida = new Path("/proyectofinalGD/salida");
        if (fs.exists(salida)) {
            fs.delete(salida, true);
        }

        Job job = Job.getInstance(conf, "Total por facturas");
        job.setJarByClass(ETL_MapReduce.class);
        job.setMapperClass(TotalFacturaMapper.class);
        job.setCombinerClass(TotalFacturaReducer.class);
        job.setReducerClass(TotalFacturaReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, detalle);
        FileOutputFormat.setOutputPath(job, salida);

        if (!job.waitForCompletion(true)) {
            System.err.println("Falló el MapReduce de total por factura.");
            System.exit(1);
        }
        System.out.println("MapReduce terminado.");

        // 4. JOB 2: JOIN Productos + FacturasDetalle
        Path salidaJoin = new Path("/proyectofinalGD/salidaJoin");
        if (fs.exists(salidaJoin)) {
            fs.delete(salidaJoin, true);
        }

        Job jobJoin = Job.getInstance(conf, "Total ventas por producto");
        jobJoin.setJarByClass(ETL_MapReduce.class);

        MultipleInputs.addInputPath(jobJoin, productos,
                TextInputFormat.class, ProductosMapper.class);
        MultipleInputs.addInputPath(jobJoin, detalle,
                TextInputFormat.class, DetalleMapper.class);

        jobJoin.setReducerClass(JoinReducer.class);

        jobJoin.setMapOutputKeyClass(Text.class);
        jobJoin.setMapOutputValueClass(Text.class);
        jobJoin.setOutputKeyClass(Text.class);
        jobJoin.setOutputValueClass(DoubleWritable.class);

        FileOutputFormat.setOutputPath(jobJoin, salidaJoin);

        if (!jobJoin.waitForCompletion(true)) {
            System.err.println("Falló el MapReduce del JOIN.");
            System.exit(1);
        }
        System.out.println("JOIN Productos + FacturasDetalle terminado.");

        // 5. Exportar resultados a MySQL
        System.out.println("Exportando resultado a MySQL...");

        int insertados;
        int insertadosJoin;

        try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPass);
             Statement stmt = conn.createStatement()) {

            stmt.executeUpdate(
                "CREATE TABLE IF NOT EXISTS total_factura (" +
                "  idFactura VARCHAR(60)," +
                "  cantidad_total DOUBLE" +
                ")");

            stmt.executeUpdate(
                "CREATE TABLE IF NOT EXISTS total_producto (" +
                "  Nombre_Producto VARCHAR(60)," +
                "  Total DOUBLE" +
                ")");

            // Limpiar datos de corridas anteriores
            stmt.executeUpdate("TRUNCATE TABLE total_factura");
            stmt.executeUpdate("TRUNCATE TABLE total_producto");
            System.out.println("Tablas listas.");

            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO total_factura (idFactura, cantidad_total) VALUES (?, ?)")) {
                insertados = cargarSalida(fs, salida, ps);
            }

            try (PreparedStatement psJoin = conn.prepareStatement(
                    "INSERT INTO total_producto (Nombre_Producto, Total) VALUES (?, ?)")) {
                insertadosJoin = cargarSalida(fs, salidaJoin, psJoin);
            }
        }

        fs.close();

        System.out.println("Registros insertados en total_factura: " + insertados);
        System.out.println("Ventas por producto insertadas: " + insertadosJoin);
        System.out.println("¡Proyecto final completado!");
    }
}