<?php

namespace Honvid\Facades;

/*
|--------------------------------------------------------------------------
| CLASS NAME: Thrift
|--------------------------------------------------------------------------
| @author    honvid
| @datetime  2018-06-07 16:34
| @package   Honvid\Facades
| @description:
|
*/

use Illuminate\Support\Facades\Facade;

class Thrift extends Facade
{
    protected static function getFacadeAccessor()
    {
        return 'thrift';
    }
}