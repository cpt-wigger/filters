from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from base import BaseFilter

SQL_Base = declarative_base()


class Filter(SQL_Base):
    """SQL模型类"""
    __tablename__ = "filter"
    id = Column(Integer, primary_key=True)
    hash_value = Column(String(40), index=True, unique=True)


class MysqlFilter(BaseFilter):
    """使用mysql存储进行去重判断"""

    def _get_storage(self):
        engine = create_engine(self.mysql_host)
        # 创建表
        SQL_Base.metadata.create_all(engine)
        Session = sessionmaker(engine)
        return Session

    def _save(self, hash_value):
        """使用mysql存储数据哈希值去重"""
        session = self.storage()
        db_filter = Filter(hash_value=hash_value)
        session.add(db_filter)
        session.commit()
        session.close()

    def _is_exists(self, hash_value):
        session = self.storage()
        ret = session.query(Filter).filter_by(hash_value=hash_value).first()
        session.close()
        if ret:
            return True
        else:
            return False
