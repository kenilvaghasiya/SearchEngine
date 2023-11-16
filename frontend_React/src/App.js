import "./app.css";
import Paper from "@mui/material/Paper";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableContainer from "@mui/material/TableContainer";
import TableHead from "@mui/material/TableHead";
import TablePagination from "@mui/material/TablePagination";
import TableRow from "@mui/material/TableRow";
import * as React from "react";
import CircularProgress from "@mui/material/CircularProgress";
import Box from "@mui/material/Box";
const columns = [{ id: "name", label: "File ID" },{ id: "label", label: "File Name" },{ id: "score", label: "Score" },{ id: "okapi", label: "Okapi" }];

const rows = [];

function App() {
  const [page, setPage] = React.useState(0);
  const [ld, setld] = React.useState(false);

  const [searchData, setsearchData] = React.useState("");
  const [Result, setResult] = React.useState([]);
  const [totalFile, settotalFile] = React.useState(0);
  const [fileSearchtype, setfileSearchtype] = React.useState("json");
  const [querySearchtype, setquery] = React.useState("BooleanQuery");

  const [rowsPerPage, setRowsPerPage] = React.useState(100);
  const query={"BooleanQuery":"http://127.0.0.1:5000/searchdata","score":"http://127.0.0.1:5000/rankquery","okapi":"http://127.0.0.1:5000/rankquery"

  }

  const handleChangePage = (event, newPage) => {
    setPage(newPage);
  };

  const handleChangeRowsPerPage = (event) => {
    setRowsPerPage(+event.target.value);
    setPage(0);
  };

  const handleSearch = async () => {
    try {
      setld(true);
      setResult([]);
      const response = await fetch(query[querySearchtype], {
        method: "POST", // Specify the HTTP method as 'POST'
        headers: {
          "Content-Type": "application/json", // Set the content type to JSON
        },
        body: JSON.stringify({ text: searchData, type: querySearchtype }), // Convert your data to JSON format
      })
        .then((res) => {
          const outputArray = res.json();
          return outputArray;
        })
        .then((res) => {
          console.log(res);
          var maindata = res["data"].map((item) => {
            return { name: item['doc_id'] , label:item['name']?item['name']:"",score:item['score']?item['score']:"",okapi:item['Okapi']?item['Okapi']:"" };
          });
          setResult(maindata);
          settotalFile(res["file"]);
          setld(false);
        });
    } catch (error) {
      console.error(error);
    }
  };

  return (
    <div className="App">
      <div className="btnSection">
        <div
          className={
            fileSearchtype === "json" ? "btnFiles actClass" : "btnFiles "
          }
          onClick={() => {
            setfileSearchtype("json");
          }}
        >
          JSON
        </div>
        <div
          className={
            fileSearchtype === "txt" ? "btnFiles actClass" : "btnFiles "
          }
          onClick={() => {
            setfileSearchtype("txt");
          }}
        >
          TEXT
        </div>
        <div
          className={
            fileSearchtype === "pdf" ? "btnFiles actClass" : "btnFiles "
          }
          onClick={() => {
            setfileSearchtype("pdf");
          }}
        >
          PDF
        </div>
        <div
          className={
            fileSearchtype === "docx" ? "btnFiles actClass" : "btnFiles "
          }
          onClick={() => {
            setfileSearchtype("docx");
          }}
        >
          DOCX
        </div>
        <div
          className={
            fileSearchtype === "xml" ? "btnFiles actClass" : "btnFiles "
          }
          onClick={() => {
            setfileSearchtype("xml");
          }}
        >
          XML / HTML
        </div>
      </div>

      <div className="btnSection">
        <div
          className={
            querySearchtype === "BooleanQuery"
              ? "btnFiles actClass"
              : "btnFiles "
          }
          onClick={() => {
            setquery("BooleanQuery");
          }}
        >
          Boolean Query
        </div>
        <div
          className={
            querySearchtype === "score"
              ? "btnFiles actClass"
              : "btnFiles "
          }
          onClick={() => {
            setquery("score");
          }}
        >
          Ranked Query
        </div>
        <div
          className={
            querySearchtype === "okapi" ? "btnFiles actClass" : "btnFiles "
          }
          onClick={() => {
            setquery("okapi");
          }}
        >
          Okapi
        </div>
      </div>
      <nav className="NavbarCSS">
        <div className="mainNavbarCSS">
          <input
            className="formSearchbar"
            type="search"
            placeholder="Search"
            onChange={(target) => setsearchData(target.target.value)}
            aria-label="Search"
          />
          <button
            className="formSearchbarBtn"
            type="submit"
            onClick={() => {
              handleSearch();
            }}
          >
            Search
          </button>
        </div>
      </nav>

      <div className="totalFileSection">
        <div>Total File :</div>
        <div>{totalFile ? totalFile : 0}</div>
      </div>
      {ld ? (
        <Box sx={{ display: "flex" }}>
          <CircularProgress />
        </Box>
      ) : (
        ""
      )}

      <Paper sx={{ width: "80%", overflow: "hidden" }} className="mainTable">
        <TableContainer sx={{ maxHeight: 650 }}>
          <Table stickyHeader aria-label="sticky table">
            <TableHead>
              <TableRow>
                {columns.map((column) => (
                  <TableCell
                    key={column.id}
                    align={column.align}
                    style={{ minWidth: column.minWidth }}
                  >
                    {column.label}
                  </TableCell>
                ))}
              </TableRow>
            </TableHead>
            <TableBody>
              {Result.slice(
                page * rowsPerPage,
                page * rowsPerPage + rowsPerPage
              ).map((row) => {
                return (
                  <TableRow hover role="checkbox" tabIndex={-1} key={row.code}>
                    {columns.map((column) => {
                      const value = row[column.id];
                      return (
                        <TableCell key={column.id} align={column.align}>
                          {column.format && typeof value === "number"
                            ? column.format(value)
                            : value}
                        </TableCell>
                      );
                    })}
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
        </TableContainer>
        <TablePagination
          rowsPerPageOptions={[10, 25, 100]}
          component="div"
          count={Result.length}
          rowsPerPage={rowsPerPage}
          page={page}
          onPageChange={handleChangePage}
          onRowsPerPageChange={handleChangeRowsPerPage}
        />
      </Paper>
    </div>
  );
}

export default App;
