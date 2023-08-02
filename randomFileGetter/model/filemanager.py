import subprocess
import threading
import random
import os
import time

import randomFileGetter.model.filemanager

class FileManager(object):
    """ コンストラクタ """
    def __init__( self, _weightDefault ) -> None:
        """ メンバ変数 """
        self.fileDB = {}
        self.isStored = False;
        self.isStoreFinished = False;
        self.weightdefault = _weightDefault;
        self.isDBMerging = False;
        self.isFileReady = False;

    """ データベースに書き込み """
    def addDB( self, _name ):
        """ ファイル名に対応するカウント値(初期値0) を代入する """
        self.fileDB[ _name ] = 3
        """ 複数のファイルを取得完了したら取得済みに設定する """
        if( 1000 < len( self.fileDB ) ):
            self.isStored = True

    """ データベースのアップデートを完了する """
    def fixDB( self ):
        self.isStored = True
        self.isStoreFinished = True

    """ データーベースからファイル名をランダムに取得 """
    def getRandomFileName( self ):
        """ ファイルが記録ずみの場合に取得する """
        if( self.isStored ):
                """ 重みづけしたランダムなファイル名を取得する """
                candidates = [*self.fileDB]
                weights = [*self.fileDB.values()]
                self.fileName = random.choices(candidates, weights=weights)[0]

                """ 1以上の時、デクリメントする """
                if( 1 < self.fileDB[ self.fileName ] ):
                    self.fileDB[ self.fileName ] -= 1
                else:
                    self.fileDB[ self.fileName ] = self.weightdefault

                return self.fileName
        else:
            raise ValueError("database is not ready")

    """ データベースファイルを取得する """
    def getDatabase( self ):
        return self.fileDB

    """ データベースファイルをマージする """
    def mergeDatabase( self, _db ):
        self.isStored = False
        self.isStoreFinished = False

        self.fileDB = _db

        self.isStored = True
        self.isStoreFinished = True

    """ データベース取得が完了したか確認する """
    def isDBCreateFinished( self ):
        return self.isStoreFinished

    """ データベースの格納数を取得する """
    def getDBIndexSize( self ):
        return len( self.fileDB )

    """ ターゲットパスを取得する """
    def getPath( self ):
        return self.destPath

""" rclone の実装 """
class RcloneFileManager(FileManager):
    """ コンストラクタ """
    def __init__( self, _rclonePath, _googleDrivePath, _weightDefault, _indexOnlyMode ) -> None:

        super().__init__( _weightDefault )

        """ メンバ変数 """
        self.rclonePath = _rclonePath
        self.googleDrivePath = _googleDrivePath
        self.indexOnlyMode = _indexOnlyMode
        self.rcloneAbort = False

        """ データベースの初期化スレッドの開始 """
        self.initDatabaseThread = threading.Thread(target=self.fileManagerInitDatabaseThread)
        self.initDatabaseThread.start()

        if( not _indexOnlyMode ):
            """ データベースの更新スレッドの開始 """
            self.updateDatabaseThread = threading.Thread(target=self.fileManagerUpdateDatabaseThread)
            self.updateDatabaseThread.start()


    """ DBイニシャライズスレッド """
    def fileManagerInitDatabaseThread( self ):
        """ rclone の実行(ls) """
        while True:
            try:
#                print("DB Init : Execute ls")
                self.proc = subprocess.Popen( [ self.rclonePath, "ls", self.googleDrivePath ], stdout=subprocess.PIPE )
                break
            except:
                pass

        """ rclone 出力を取得して fileDB に格納 """
        while True:

            self.line = self.proc.stdout.readline()

            if self.line:
                self.s = self.line.split()
                self.addDB( self.s[1].decode() )

            if not self.line and self.proc.poll() is not None:

                if( 0 != self.proc.returncode ):
                    if( self.indexOnlyMode ):
#                        print("DB Init : Error. Index generate abort.")
                        self.rcloneAbort = True
                    else:
                        raise ValueError("rclone execute error")

                self.fixDB()
#                print("DB Init done")
                break

    """ DB更新スレッド """
    def fileManagerUpdateDatabaseThread( self ):
        """ DB構築完了まで待つ """
        while( not self.isDBCreateFinished() ):
            time.sleep( 1 )

        while True:
            """ 指定の期間のウエイト後、クラウドファイルDBをアップデートする """
            time.sleep( 604800 )
            
            """ DBアップデート用のインスタンス生成 """
#                fileManagerForUpdate = randomFileGetter.model.filemanager.RcloneFileManager( self.rclonePath, self.googleDrivePath, self.weightdefault, True )
            fileManagerForUpdate = randomFileGetter.model.filemanager.RcloneFileManager( self.rclonePath, "aaaaa" , self.weightdefault, True )

            """ DB生成完了まで待つ """
            while( not fileManagerForUpdate.isDBCreateFinished() ):
                time.sleep( 1 )
                pass            
                
            """ DBのマージを実行 """
            if ( not fileManagerForUpdate.isRcloneExecuteAborted() ):
                self.mergeDatabase( fileManagerForUpdate.getDatabase() )

            del fileManagerForUpdate


    """ 実際のファイルを取得(1ファイル) """
    def getActualFile( self, _fileName, _storePath ):
        fullPath = self.googleDrivePath + '/' + _fileName
        while True:
            try:
#                print("Download :", fullPath)
                subprocess.call([ self.rclonePath , "copy", fullPath, _storePath ])
                break
            except:
                raise ValueError("rclone copy error")
        return _fileName

    """ 実際のファイルを削除 """
    def deleteActualFile( self, _fileName ):
        try:
#            print("delete :", _fileName )
            self.proc = subprocess.Popen( [ self.rclonePath, "delete", _fileName,  ], stdout=subprocess.PIPE )
        except:
            pass

    """ rcloneの実行が中断したか取得する """
    def isRcloneExecuteAborted( self ):
        return self.isStoreFinished

""" ローカルファイル の実装 """
class LocalFileManager(FileManager):
    """ コンストラクタ """
    def __init__(self, _lsPath, _destPath, _weightDefault, _indexOnlyMode ) -> None:

        super().__init__( _weightDefault )

        """ メンバ変数 """
        self.lsPath = _lsPath
        self.destPath = _destPath

        """ データベースの初期化スレッドの開始 """
        self.initDatabaseThread = threading.Thread(target=self.fileManagerInitDatabaseThread)
        self.initDatabaseThread.start()

        if( not _indexOnlyMode ):
            """ データベースの更新スレッドの開始 """
            self.updateDatabaseThread = threading.Thread(target=self.fileManagerUpdateDatabaseThread)
            self.updateDatabaseThread.start()


    """ 実際のファイルを削除 """
    def deleteActualFile( self, _fileName, _storePath ):
        try:
            delfile = _storePath + "/" + _fileName
#            print("Delete :", delfile)
            os.remove( delfile )

            self.fileDB.pop(_fileName)

        except:
            """ 削除エラーが発生したときはリストのみ削除する """
            pass

    """ DBイニシャライズスレッド """
    def fileManagerInitDatabaseThread( self ):
        """ rclone の実行(ls) """
        while True:
            try:
                self.proc = subprocess.Popen( [ self.lsPath, "-1", self.destPath ], stdout=subprocess.PIPE )
                break
            except:
                pass

        """ rclone 出力を取得して fileDB に格納 """
        while True:
            self.line = self.proc.stdout.readline()

            if self.line:
                self.s = self.line.split()
                self.addDB( self.s[0].decode() )

            if not self.line and self.proc.poll() is not None:
                self.fixDB()
                break

    """ DB更新スレッド """
    def fileManagerUpdateDatabaseThread( self ):
        """ DB構築完了まで待つ """
        while( not self.isDBCreateFinished() ):
            time.sleep( 1 )

        while True:
            """ 指定の期間のウエイト後、クラウドファイルDBをアップデートする """
            time.sleep( 5 )
            
            """ DBアップデート用のインスタンス生成 """
            fileManagerForUpdate = randomFileGetter.model.filemanager.LocalFileManager( self.lsPath, self.destPath, self.weightdefault, True )

            """ DB生成完了まで待つ """
            while( not fileManagerForUpdate.isDBCreateFinished() ):
                pass            
                
            """ DBのマージを実行 """
            self.mergeDatabase( fileManagerForUpdate.getDatabase() )
            del fileManagerForUpdate
