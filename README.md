
注意：

1、设置环境变量PYTHON_PROJECT_ENV（可选的值：product、test、local）


2、自动根据mysql源表来生成yaml文件

cd etl_schedule/load

python mysql2yaml.py -d db_level -t tb_level_guid -p .

生成的文件保存在gen目录下

schema－－－保存了生成的创建表的语句，类型需要根据具体情况来进行调整

script－－－保存了生成的yml文件

sql－－－保存了从ods到dim、fact的sql文件


3、yml文件的执行

python -u /data/soft/etl_schedule/bin/runyaml.py -p /data/soft/etl_schedule/load/gen/script/ods_mysql/ods_db_level__tb_level_guid.yml
