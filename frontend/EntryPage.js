function fetchAndStoreWordlist() {
  fetch("/indexlist")
    .then((response) => {
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      return response.json();
    })
    .then((data) => {
      localStorage.setItem("wordlist", JSON.stringify(data));
    })
    .catch((error) => {
      console.error("Error fetching wordlist:", error);
    });
}

document.addEventListener("DOMContentLoaded", function () {
  fetchAndStoreWordlist();
});

function performSearch() {
  const query = document.getElementById("searchInput").value
  localStorage.setItem("searchQuery", query);
  window.location.href = "ResultsPage.html";
}

function onImageLoad() {
  //console.log("Image has loaded!");
  document.body.style.backgroundColor = "#f0f0f0";
}

document.addEventListener("DOMContentLoaded", function () {
  const searchInput = document.getElementById("searchInput");
  const resultsContainer = document.getElementById("autocompleteResults");

  const searchForm = document.querySelector(".search-form");
  searchForm.addEventListener("submit", function (event) {
    event.preventDefault();
    performSearch();
  });

  searchInput.addEventListener("input", function (e) {

    const suggestions = JSON.parse(localStorage.getItem("wordlist")) || [];
    //console.log(suggestions);
    const inputValue = e.target.value;

    resultsContainer.innerHTML = "";
    resultsContainer.style.display = "none";

    if (inputValue) {
      suggestions[0]
        .filter(function (item) {
          return item.toLowerCase().startsWith(inputValue.toLowerCase());
        })
        .slice(0, 10)
        .forEach(function (suggested) {
          const div = document.createElement("div");
          div.textContent = suggested;
          div.className = "suggestion";
          div.addEventListener("click", function () {
            searchInput.value = suggested;
            resultsContainer.style.display = "none";
          });
          resultsContainer.appendChild(div);
        });

      if (resultsContainer.childElementCount > 0) {
        resultsContainer.style.display = "block";
      } else {
        const suggestion = spellcheck(inputValue, suggestions[0]);

        if (suggestion && levenshteinDistance(inputValue, suggestion) <= 2) {

          //console.log("Did you mean:", suggestion);

          const div = document.createElement("div");
          div.textContent = suggestion;
          div.className = "suggestion";
          div.addEventListener("click", function () {
            searchInput.value = suggestion;
            resultsContainer.style.display = "none";
          });
          resultsContainer.appendChild(div);
          resultsContainer.style.display = "block";
        }
      }
    }
  });

  document.addEventListener("click", function (e) {
    if (e.target !== searchInput) {
      resultsContainer.style.display = "none";
    }
  });
});

document.addEventListener("DOMContentLoaded", function () {
  const searchButton = document.getElementById("searchButton");

  searchButton.addEventListener("mouseout", function () {
    searchButton.textContent = "Search";
  });
});

function levenshteinDistance(a, b) {
  const matrix = [];
  for (let i = 0; i <= b.length; i++) {
    matrix[i] = [i];
  }
  for (let j = 0; j <= a.length; j++) {
    matrix[0][j] = j;
  }
  for (let i = 1; i <= b.length; i++) {
    for (let j = 1; j <= a.length; j++) {
      if (b.charAt(i - 1) == a.charAt(j - 1)) {
        matrix[i][j] = matrix[i - 1][j - 1];
      } else {
        matrix[i][j] = Math.min(
          matrix[i - 1][j - 1] + 1,
          Math.min(matrix[i][j - 1] + 1, matrix[i - 1][j] + 1)
        );
      }
    }
  }
  return matrix[b.length][a.length];
}

function spellcheck(input, dictionary) {
  let minDistance = Infinity;
  let closestWord = "";
  for (const word of dictionary) {
    const distance = levenshteinDistance(input, word);
    if (distance < minDistance) {
      minDistance = distance;
      closestWord = word;
    }
  }

  return closestWord;
}
