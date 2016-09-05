
JDK1.7+ required

install eaux to local Maven Repo
```
cd ..
mvn install -Dmaven.test.skip=true

```

generate App assembler
```
cd ./example
mvn clean package appassembler:assemble -Dmaven.test.skip=true
```

run it
```
cd target/appassembler
bin/example.sh 
```



