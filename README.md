# thrift-laravel

## 安装
    
    composer require honvid/thrift-hbase-laravel

在项目中 `config/app.php` 中添加如下
    
    'providers' => [
        ...
        LinkDoc\Providers\ThriftServiceProvider::class    
    ],
    'aliases' => [
        ...
        'Thrift' => LinkDoc\Facades\Thrift::class
    ]

最后在命令后执行

    php artisan vendor:publish --provider="LinkDoc\Providers\ThriftServiceProvider"
    
在 `config` 目录中会生成 `thrift.php` 文件，修改对应配置即可。
    
## 使用
    
    use Thrift;
    
    ...
    
    
    Thrift::getValue('table1', 'ccolumn1', 'row1');
