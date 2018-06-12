# thrift-hbase-laravel

## 安装|Install
    
    composer require honvid/thrift-hbase-laravel

在项目中 `config/app.php` 中添加如下
    
    'providers' => [
        ...
        Honvid\Providers\ThriftServiceProvider::class    
    ],
    'aliases' => [
        ...
        'Thrift' => Honvid\Facades\Thrift::class
    ]

最后在命令后执行

    php artisan vendor:publish --provider="Honvid\Providers\ThriftServiceProvider"
    
在 `config` 目录中会生成 `thrift.php` 文件，修改对应配置即可。
    
## 使用|Use Example 
    
    use Thrift;
    
    ...
    
    
    Thrift::getValue('table1', 'ccolumn1', 'row1');
