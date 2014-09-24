s3cp
====

* s3へのrsyncのようにディレクトリ丸ごとアップロードします。
* アップロードの際、並列で複数のファイルを同時にアップロードすることが可能です。



環境変数
--------

実行前にAWSのAccess keyとSecret access keyを環境変数にセットする必要があります。

```bash:
export AWS_ACCESS_KEY_ID="hoge"
export AWS_SECRET_ACCESS_KEY="fuga"
````

使い方
------


s3cp [options] <ローカルのディレクトリパス> <バケット名> <S3のディレクトリパス>


* options:
 * -checkmd5=false:
   * 同名のファイルが既に存在する場合にMD5sumを検証し、異なる場合のみ上書
 * -checksize=true:
   * 同名のファイルが既に存在する場合にファイルサイズを検証し、異なる場合のみ上書
 * -n=1:
   * 並列アップロードする数(デフォルト:1)
 * -region=ap-northeast-1:
   * 対象リージョンの指定

