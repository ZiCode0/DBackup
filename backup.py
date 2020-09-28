import os
import datetime
import daemon
import time
import backup_config as conf


class MySQLBackup:
    def __init__(self):
        self.db_user = conf.db_user
        self.db_pass = conf.db_pass
        self.db_name = conf.db_name
        self.tg_token = conf.tg_token
        self.chat_id = conf.chat_id
        self.backup_folder = conf.backup_folder
        self.wait_time = conf.wait_time

        self.bckp_filename = ''

    @staticmethod
    def generate_backup_filename():
        """
        Generate backup filename with extension
        Using datetime snapshot as filename + .sql
        :return: string
        """
        return str(datetime.datetime.now()).replace(' ', '+').replace(':', '') + '.sql'

    def make_backup(self):
        """
        Make fast database backup as sql file.
        Using mysqldump utility.
        :return: None
        """
        self.bckp_filename = self.generate_backup_filename()
        os.system('mysqldump --user {user} --password={password} {db_name} > {output}'.format(user=self.db_user,
                                                                                              password=self.db_pass,
                                                                                              db_name=self.db_name,
                                                                                              output=os.path.join(
                                                                                                  self.backup_folder,
                                                                                                  self.bckp_filename))
                  )

    def send_backup(self):
        """
        Send backup to target Telegram chat using Telegram Bot API.
        :return: None
        """
        os.system('curl -v -F "chat_id={chat_id}" -F document=@{file_path} '
                  'https://api.telegram.org/bot{token}/sendDocument'.format(chat_id=self.chat_id,
                                                                            file_path=os.path.join(self.backup_folder,
                                                                                                   self.bckp_filename),
                                                                            token=self.tg_token))

    def main(self):
        """
        Main loop function to make backup each wait_time interval
        :return: None
        """
        while True:
            self.make_backup()  # create .sql backup
            self.send_backup()  # send using tg bot API
            time.sleep(self.wait_time)  # waiting interval


if __name__ == '__main__':
    bk = MySQLBackup()
    # run script in background
    with daemon.DaemonContext():
        bk.main()
