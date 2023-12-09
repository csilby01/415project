<template>
  <form @submit="handleSubmit">
    <div class="container">
      <div class="box">
        <div>
          <p>Type</p>
          <select v-model="formData.source.type" required id="srcSelect">
            <option value="city">City</option>
            <option value="country">Country</option>
            <option value="airport">Airport</option>
          </select>
        </div>
      <div>
        <p>Source location</p>
        <input type="text" placeholder="name/IATA/ICAO" required v-model="formData.source.value" id="srcInput">
      </div>
    </div>  
    <div class="box">
      <div>
        <p>Type</p>
        <select v-model="formData.destination.type" required id="desSelect">
          <option value="city">City</option>
          <option value="country">Country</option>
          <option value="airport">Airport</option>
        </select>
      </div>
      <div>
        <p>Destination location</p>
        <input type="text" placeholder="name/IATA/ICAO" required v-model="formData.destination.value" id="desInput">
      </div>
    </div>
    </div>
    <br>
    <div class="container">
      <div class="box">
        <p>Other filters:</p>
        <div class="filter-container">
          <p>Plane models</p>
          <input v-model="formData.filters.plane" type="text" placeholder="name/IATA/ICAO">
        </div>
        <div class="filter-container">
          <p>Airline</p>
          <input v-model="formData.filters.airline" type="text" placeholder="name/IATA/ICAO">
        </div>
        <div class="checkbox-container">
          <input v-model="formData.filters.direct_only" type="checkbox" id="myCheckbox" />
          <label for="myCheckbox">Direct only</label>
        </div>
      </div>
    </div>
    <br>
    <button type="submit">Search</button>
  </form>
  <div v-if="loading" class="loading-container">
    <div class="loader"></div>
  </div>
  <div v-if="items.length > 0" class="results-container">
    <div>
      <h2 style="text-align: left;">Results</h2>
      <table>
        <thead>
          <tr>
            <!-- Dynamically create table headers -->
            <th v-for="(value, key) in items[0]" :key="key">
              {{ capitalizeWords(key.replaceAll("_"," ")) }}
            </th>
          </tr>
        </thead>
        <tbody>
          <!-- Dynamically create table rows -->
          <tr v-for="(item, index) in paginatedItems" :key="index">
            <td style="font-size: 12px;" v-for="(value, key) in item" :key="key">
              {{ value }}
            </td>
          </tr>
        </tbody>
      </table>
      <br>
      <div class="buttons-container">
        <button @click="prevPage" :disabled="currentPage <= 1">Prev</button>
        <span>Page {{ currentPage }} of {{ totalPages }}</span>
        <button @click="nextPage" :disabled="currentPage >= totalPages">Next</button>
      </div>
    </div>
  </div>
</template>

<script>
export default {
  name: 'HelloWorld',
  props: {
    msg: String
  },
  data() {
      return {
        currentPage: 1,
        itemsPerPage: 10,
        formData:{
          "source": {
            "type":"",
            "value": ""
          },
          "destination":{
            "type":"",
            "value": ""
          },
          "filters":{
            "plane": "",
            "airline": "",
            "direct_only": false
          }
        },
        items: [],
        loading: false
      }
  },
  computed: {
    totalPages() {
      return Math.ceil(this.items.length / this.itemsPerPage);
    },
    paginatedItems() {
      let start = (this.currentPage - 1) * this.itemsPerPage;
      let end = start + this.itemsPerPage;
      return this.items.slice(start, end);
    }
  },
  methods: {
    nextPage() {
      if (this.currentPage < this.totalPages) this.currentPage++;
    },
    prevPage() {
      if (this.currentPage > 1) this.currentPage--;
    },
    handleSubmit(event)
    {
      event.preventDefault();
      var jsonData = JSON.stringify(this.formData);
      this.loading = true;
      this.items = [];
      this.currentPage = 1;
      fetch("http://localhost:3000/find", {
          method: "POST",
          headers: {
              "Content-Type": "application/json", 
          },
          body: jsonData
      })
      .then(response => response.json())
      .then(data => {
          this.items = data;
          this.loading = false;
      });
    },capitalizeWords(str) {
      // Split the string into words
      let words = str.split(' ');

      // Iterate through each word
      for (let i = 0; i < words.length; i++) {
          let word = words[i];
          
          // Uppercase the first letter and lowercase the rest of the letters
          words[i] = word.charAt(0).toUpperCase() + word.slice(1).toLowerCase();
      }

      // Join the words back together into a single string
      return words.join(' ');
    }
  }
}
</script>

<!-- Add "scoped" attribute to limit CSS to this component only -->
<style scoped>
.checkbox-container {
    display: flex;
    flex-direction: column;
    align-items: center; /* This aligns the checkbox and label vertically */
}

.box{
  gap: 10px;
  display: flex;
  align-items: center;
}

.box p{
  margin: 0;
}

.container{
  margin: auto;
  gap: 100px;
  display: flex;
  align-items: center; /* This aligns the checkbox and label vertically */
  align-content: center;
  justify-content: center;
}

.filter-container p{
  margin: 0;
}

table {
  width: 100%;
  border-collapse: collapse;
}

.results-container{
  display: flex;
  align-items: center; /* This aligns the checkbox and label vertically */
  align-content: center;
  justify-content: center;
}

th, td {
  border: 1px solid #ddd;
  padding: 8px;
  text-align: left;
}

.buttons-container{
  display: flex;
  gap: 10px;
  align-items: center; /* This aligns the checkbox and label vertically */
  align-content: center;
  justify-content: center;
}

.loader {
  border: 16px solid #f3f3f3; /* Light grey */
  border-top: 16px solid #69bff8; /* Blue */
  border-radius: 50%;
  width: 30px;
  height: 30px;
  animation: spin 2s linear infinite;
}

@keyframes spin {
  0% { transform: rotate(0deg); }
  100% { transform: rotate(360deg); }
}

.loading-container{
  display: flex;
  margin-top: 50px;
  align-items: center; /* This aligns the checkbox and label vertically */
  align-content: center;
  justify-content: center;
}
</style>
