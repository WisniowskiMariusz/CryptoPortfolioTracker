from app.base import Base

def test_base_is_declarative_base():
    # Base should have metadata and registry attributes typical for declarative_base
    assert hasattr(Base, "metadata")
    assert hasattr(Base, "registry")
    # metadata should be an instance of sqlalchemy MetaData
    from sqlalchemy.schema import MetaData
    assert isinstance(Base.metadata, MetaData)