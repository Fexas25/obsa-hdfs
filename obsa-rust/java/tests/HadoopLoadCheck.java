import java.util.jar.*;
import java.util.*;
/** Load, link and initialize every adapter class against the Hadoop on the classpath. */
public class HadoopLoadCheck {
  public static void main(String[] args) throws Exception {
    List<String> failures = new ArrayList<>();
    int loaded = 0;
    try (JarFile jar = new JarFile(args[0])) {
      for (Enumeration<JarEntry> e = jar.entries(); e.hasMoreElements(); ) {
        String name = e.nextElement().getName();
        if (!name.endsWith(".class") || name.endsWith("NativeObs.class")) continue;
        String cls = name.replace('/', '.').replace(".class", "");
        try {
          Class<?> c = Class.forName(cls, true, HadoopLoadCheck.class.getClassLoader());
          c.getDeclaredMethods(); // resolve method signatures too
          loaded++;
        } catch (Throwable t) {
          failures.add(cls + " -> " + t);
        }
      }
    }
    System.out.println("loaded=" + loaded + " failures=" + failures.size());
    failures.forEach(System.out::println);
  }
}
