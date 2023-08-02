import time
import randomFileGetter.model.filemanager

class controller(object):
    def __init__( self, config ) -> None:
        """ Config 内容の読み込み """
        self.sourcePath = config['DEFAULT']['googleDrivePath']
        self.destPath = config['DEFAULT']['storePath']
        self.randomWeightdefault = int(config['DEFAULT']['randomWeightdefault'])
        self.rclonePath = config['DEFAULT']['rclonePath']
        self.lsPath = config['DEFAULT']['lsPath']

        """ source file manager の起動 """
        self.sourceFileManager = randomFileGetter.model.filemanager.RcloneFileManager( self.rclonePath, self.sourcePath, self.randomWeightdefault, False )

        """ dest file manager の起動 """
        self.destFileManager = randomFileGetter.model.filemanager.LocalFileManager( self.lsPath, self.destPath, self.randomWeightdefault, False )

    def startup( self ):
        while True:
            time.sleep(1)

            """ 準備完了時のみ実行 """
            if( self.sourceFileManager.isDBCreateFinished() and self.destFileManager.isDBCreateFinished() ):
                """ ソースからファイルをダウンロード """
                while ( self.destFileManager.getDBIndexSize() <= 500 ):
                    self.destFileManager.addDB( self.sourceFileManager.getActualFile( self.sourceFileManager.getRandomFileName(), self.destFileManager.getPath() ) )

                if( self.destFileManager.getDBIndexSize() > 500 ):
                    """ ファイルがいっぱいであれば削除 """
                    while ( self.destFileManager.getDBIndexSize() > 500 ):
                        """ ランダムにファイルを削除 """
                        self.destFileManager.deleteActualFile( self.destFileManager.getRandomFileName(), self.destFileManager.getPath() )
                    """ ファイルがいっぱいのときは指定時間まつ """
                    time.sleep(7200)

