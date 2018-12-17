package mydump_test

import (
	"io/ioutil"
	"os"

	. "github.com/pingcap/check"
	. "github.com/pingcap/tidb-lightning/lightning/mydump"
)

//////////////////////////////////////////////////////////

var _ = Suite(&testMydumpReaderSuite{})

type testMydumpReaderSuite struct{}

func (s *testMydumpReaderSuite) SetUpSuite(c *C)    {}
func (s *testMydumpReaderSuite) TearDownSuite(c *C) {}

func (s *testMydumpReaderSuite) TestExportStatementNoTrailingNewLine(c *C) {
	file, err := ioutil.TempFile("", "tidb_lightning_test_reader")
	c.Assert(err, IsNil)
	defer os.Remove(file.Name())

	_, err = file.Write([]byte("CREATE DATABASE whatever;"))
	c.Assert(err, IsNil)
	err = file.Close()
	c.Assert(err, IsNil)

	data, err := ExportStatement(file.Name(), "auto")
	c.Assert(err, IsNil)
	c.Assert(data, DeepEquals, []byte("CREATE DATABASE whatever;"))
}

func (s *testMydumpReaderSuite) TestExportStatementWithComment(c *C) {
	file, err := ioutil.TempFile("", "tidb_lightning_test_reader")
	c.Assert(err, IsNil)
	defer os.Remove(file.Name())

	_, err = file.Write([]byte(`
		/* whatever blabla 
			multiple lines comment
			multiple lines comment
			multiple lines comment
			multiple lines comment
			multiple lines comment
		 */;
		CREATE DATABASE whatever;  
`))
	c.Assert(err, IsNil)
	err = file.Close()
	c.Assert(err, IsNil)

	data, err := ExportStatement(file.Name(), "auto")
	c.Assert(err, IsNil)
	c.Assert(data, DeepEquals, []byte("CREATE DATABASE whatever;"))
}

func (s *testMydumpReaderSuite) TestExportStatementWithCommentNoTrailingNewLine(c *C) {
	file, err := ioutil.TempFile("", "tidb_lightning_test_reader")
	c.Assert(err, IsNil)
	defer os.Remove(file.Name())

	_, err = file.Write([]byte(`
		/* whatever blabla 
			multiple lines comment
			multiple lines comment
			multiple lines comment
			multiple lines comment
			multiple lines comment
		 */;
		CREATE DATABASE whatever;`))
	c.Assert(err, IsNil)
	err = file.Close()
	c.Assert(err, IsNil)

	data, err := ExportStatement(file.Name(), "auto")
	c.Assert(err, IsNil)
	c.Assert(data, DeepEquals, []byte("CREATE DATABASE whatever;"))
}

func (s *testMydumpReaderSuite) TestExportStatementGBK(c *C) {
	file, err := ioutil.TempFile("", "tidb_lightning_test_reader")
	c.Assert(err, IsNil)
	defer os.Remove(file.Name())

	_, err = file.Write([]byte("CREATE TABLE a (b int(11) COMMENT '"))
	c.Assert(err, IsNil)
	// "D7 DC B0 B8 C0 FD" is the GBK encoding of "总案例".
	_, err = file.Write([]byte{0xD7, 0xDC, 0xB0, 0xB8, 0xC0, 0xFD})
	c.Assert(err, IsNil)
	_, err = file.Write([]byte("');\n"))
	c.Assert(err, IsNil)
	err = file.Close()
	c.Assert(err, IsNil)

	data, err := ExportStatement(file.Name(), "auto")
	c.Assert(err, IsNil)
	c.Assert(data, DeepEquals, []byte("CREATE TABLE a (b int(11) COMMENT '总案例');"))
}

func (s *testMydumpReaderSuite) TestExportStatementGibberishError(c *C) {
	file, err := ioutil.TempFile("", "tidb_lightning_test_reader")
	c.Assert(err, IsNil)
	defer os.Remove(file.Name())

	_, err = file.Write([]byte("\x9e\x02\xdc\xfbZ/=n\xf3\xf2N8\xc1\xf2\xe9\xaa\xd0\x85\xc5}\x97\x07\xae6\x97\x99\x9c\x08\xcb\xe8;"))
	c.Assert(err, IsNil)
	err = file.Close()
	c.Assert(err, IsNil)

	data, err := ExportStatement(file.Name(), "auto")
	c.Assert(data, IsNil)
	c.Assert(err, NotNil)
}
