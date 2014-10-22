s3cp
====

[![Build Status](https://drone.io/github.com/masahide/s3cp/status.png)](https://drone.io/github.com/masahide/s3cp/latest)

* S3へのrsyncのようにディレクトリ丸ごとアップロードします。
* アップロードの際、並列で複数のファイルを同時にアップロードすることが可能です。
* 既にアップロード済みのファイルがある場合は、ファイルサイズもしくはmd5sum(オプションで指定可)で検証し、異なる場合は上書きでアップロードします。

Download
--------

https://drone.io/github.com/masahide/s3cp/files


注意
----

* 現状ACLは固定ですべてprivateになります。
* S3からのダウンロード機能は未実装です。
* 単独ファイルのアップロードは出来ません。対象として指定できるのはディレクトリだけです。
* シンボリックリンクは追跡します。(循環参照を回避するため、symlinkはn階層でストップします)
* Windowsはまだ未対応




環境変数
--------

実行前にAWSのAccess keyとSecret access keyを環境変数にセットする必要があります。

```bash:
export AWS_ACCESS_KEY_ID="hoge"
export AWS_SECRET_ACCESS_KEY="fuga"
````

使い方
------

```bash:
$ s3cp [options] <ローカルのディレクトリパス> <バケット名> <S3のディレクトリパス>
```

* options:
 *  -r
   *  ディレクトリコピーモード
 * -checkmd5=false:
   * 同名のファイルが既に存在する場合にMD5sumを検証し、異なる場合のみ上書
 * -checksize=true:
   * 同名のファイルが既に存在する場合にファイルサイズを検証し、異なる場合のみ上書
 * -n=1:
   * 並列アップロードする数(デフォルト:1)
 * -region=ap-northeast-1:
   * 対象リージョンの指定
 *  -jsonLog
   * 出力形式をjsonに
 *  -version
   * versionの表示
 *  -RetryInitialInterval=500: Retry Initial Interval
 *  -RetryMaxElapsedTime=15: Retry Max Elapsed Time
 *  -RetryMaxInterval=60: Retry Max Interval
 *  -RetryMultiplier=1.5: Retry Multiplier
 *  -RetryRandomizationFactor=0.5: Retry Randomization Factor
 *  -d=0: log level

