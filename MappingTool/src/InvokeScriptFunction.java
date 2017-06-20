import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import javax.script.*;

public class InvokeScriptFunction {
public static void main(String[] args) throws Exception {
    ScriptEngineManager manager = new ScriptEngineManager();
    ScriptEngine engine = manager.getEngineByName("JavaScript");

    engine.eval(Files.newBufferedReader(Paths.get("test.js"), StandardCharsets.UTF_8));

    Invocable inv = (Invocable) engine;
/*
 * 
var factory = 'factoryA';

factories[factory].method();


 */
    inv.invokeFunction("validation","alphanumber" );  //This one works.      
 }
}